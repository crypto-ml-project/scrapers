import { PrismaClient } from "@prisma/client";
import { ChildProcessWithoutNullStreams, spawn } from "child_process";
import { MessageBuilder, Webhook } from "discord-webhook-node";
import fs from "fs-extra";
import keywords from "../keywords.json";

const prisma = new PrismaClient();
const hook = new Webhook(
  "https://discord.com/api/webhooks/1057807224182931556/XIvU0Mi7yKGGhBu7gJH7sYAgn68Jo7fLSVorots5m7CdzCquHCR03sVsz--uA6KxDNkU"
);

let chunk: any[] = [];
let saving = false;

let tweetCount = 0;
let duplicateCount = 0;

let childProcess: ChildProcessWithoutNullStreams;

async function main() {
  for (let [coin, keys] of Object.entries(keywords.twitter)) {
    for (let keyword of keys) {
      // Start scraping
      scrape(keyword, coin);
      // Block here till we have too many duplicates
      await waitForDuplicates();
      await sendProgressNotification(coin, keyword);
      // Kill the scraper process
      childProcess.kill();
    }
  }
}
main();

function scrape(keyword: string, coin: string) {
  tweetCount = 0;
  duplicateCount = 0;

  childProcess = spawn("snscrape", [
    "-n 100000",
    "--jsonl",
    "twitter-search",
    keyword,
  ]);

  childProcess.stderr.on("data", (data: any) => {
    console.error(`stderr: ${data}`);
    fs.appendFile("errors.txt", `${new Date()} ${data}`);
    hook.error("**Error Processing Tweets:**", `${data}`);
  });

  childProcess.stdout.on("data", async (data: any) => {
    let tmpTweets = data.toString().split(/\n/g);
    tmpTweets = tmpTweets.filter((tweet: any) => tweet !== "");

    const tweets = tmpTweets
      .slice(0, tmpTweets.length - 1)
      .map((tweet: any) => {
        const data = JSON.parse(tweet);
        data.coin = coin;
        return;
      });

    chunk.push(...tweets);
  });
}

async function saveChunk() {
  saving = true;

  try {
    const txs: any = [];

    // Clone chunk array
    const cclone = JSON.parse(JSON.stringify(chunk));
    tweetCount += cclone.length;
    // Clear original chunk array, because it will be filled again while we are saving
    chunk = [];

    cclone.forEach(async (tweet: any) => {
      const postId = `twitter_${tweet.url.split("/")[5]}_${tweet.coin}`;

      // Rly fucking slow
      const postCheck = await prisma.socialMediaPosts.findUnique({
        where: {
          id: postId,
        },
      });
      if (postCheck) {
        duplicateCount++;
        return;
      }

      //currentIntervalCount++;
      //totalIntervalCount++;

      const userData = {
        userId: tweet.user.username || "",
        created_at: tweet.user.created
          ? new Date(tweet.user.created)
          : new Date(),
        follower: tweet.user.followersCount || 0,
        description: tweet.user.description || "",
      };

      const tx = await prisma.user.upsert({
        where: {
          userId: userData.userId,
        },
        create: userData,
        update: userData,
      });

      txs.push(tx);

      const tweetData = {
        id: postId,
        content: tweet.content || "",
        timeStamp: tweet.date ? new Date(tweet.date) : new Date(),
        like: tweet.likeCount || 0,
        comments: tweet.replyCount || 0,
        platformId: "twitter",
        tags: tweet.hashtags ? tweet.hashtags : [],
        userId: tweet.user.username || "",
        coin: tweet.coin,
      };

      const tx2 = await prisma.socialMediaPosts.upsert({
        where: {
          id: tweetData.id,
        },
        create: tweetData,
        update: tweetData,
      });

      txs.push(tx2);
    });

    await prisma.$transaction(txs);
  } catch (err) {
    console.log(err);
  }

  saving = false;
}

setInterval(async () => {
  if (saving || !chunk.length) return;
  await saveChunk();
}, 100);

setInterval(async () => {
  const msg =
    "⚙️ Tweets Processed: " +
    tweetCount +
    " | Duplicates: " +
    duplicateCount +
    " | Created: " +
    (tweetCount - duplicateCount);
  const date = new Date();
  console.log(date.toLocaleDateString(), date.toLocaleTimeString(), msg);
}, 5000);

// setInterval(async () => {
//   if (duplicateCount >= currentIntervalCount) {
//     await handleDuplicateCount().then(() => {
//       process.exit();
//     });
//   }
// }, 600000);

function waitForDuplicates() {
  return new Promise((resolve) => {
    const i = setInterval(() => {
      if (tweetCount / duplicateCount > 0.2) {
        clearInterval(i);
        resolve(null);
      }
    }, 1000 * 60 * 10);
  });
}

async function sendProgressNotification(coin: string, keyword: string) {
  const msg =
    "More than 20% of the tweets processed in the last 10 minutes were duplicates. Moving to next keyword...";
  console.log(msg);

  const statusEmbed = buildStatusEmbed(
    msg,
    tweetCount.toLocaleString(),
    duplicateCount.toLocaleString(),
    (tweetCount - duplicateCount).toLocaleString(),
    coin,
    keyword
  );
  await hook.send(statusEmbed);
}

function buildStatusEmbed(
  description: string,
  tweetCount: string,
  duplicateCount: string,
  intervalCount: string,
  coin: string,
  keyword: string
) {
  return new MessageBuilder()
    .setTitle("Status Update")
    .setDescription(description)
    .addField("📜Current Keyword", keyword, true)
    .addField("⚙️ Tweets Processed:", `${tweetCount}`, true)
    .addField("⚠️ Duplicates:", `${duplicateCount}`, true)
    .addField("✅Created", `${intervalCount}`, true)
    .setColor(1)
    .setFooter(`${coin}`)
    .setTimestamp();
}
