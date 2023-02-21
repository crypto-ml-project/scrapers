import { PrismaClient } from "@prisma/client";
import { ChildProcessWithoutNullStreams, spawn } from "child_process";
import { MessageBuilder, Webhook } from "discord-webhook-node";
import fs from "fs-extra";
import keywords from "../keywords.json";

const prisma = new PrismaClient();
const hook = new Webhook(
  "https://discord.com/api/webhooks/1057807224182931556/XIvU0Mi7yKGGhBu7gJH7sYAgn68Jo7fLSVorots5m7CdzCquHCR03sVsz--uA6KxDNkU"
);

let coin: string;
let keyword: string;

let chunk: any[] = [];
let saving = false;
let tweetCount = 0;
let duplicateCount = 0;
let createdCount = 0;
let childProcess: ChildProcessWithoutNullStreams;

enum ContinueReason {
  TOO_MANY_DUPLICATES,
  NO_NEW_TWEETS,
}

async function main() {
  setInterval(async () => {
    if (saving || !chunk.length) return;
    await saveChunk();
  }, 100);

  // Console logs
  setInterval(async () => {
    const msg =
      "Tweets Processed: " +
      tweetCount +
      " | Duplicates: " +
      duplicateCount +
      " | Created: " +
      createdCount;
    const date = new Date();
    console.log(date.toLocaleDateString(), date.toLocaleTimeString(), msg);
  }, 5000);

  // Hourly discord notifications
  setInterval(async () => {
    await sendNotification("Scraping 😎...");
  }, 1000 * 60 * 60);

  for (let [c, keys] of Object.entries(keywords.twitter)) {
    for (let key of keys) {
      // Start scraping
      coin = c;
      keyword = key;
      scrapeNext();
      // Block here till we have too many duplicates or no new tweets
      const continueReason = await wait();
      let msg;
      if (continueReason === ContinueReason.TOO_MANY_DUPLICATES) {
        msg =
          "More than 20% of the tweets processed in the last 10 minutes were duplicates. Moving to next keyword...";
      } else {
        msg = "No new tweets in the last 10 minutes. Moving to next keyword...";
      }
      console.log(msg);
      await sendNotification(msg);
      // Kill the scraper process
      childProcess.kill();
    }
  }
}
main();

function scrapeNext() {
  console.log("Scraping", keyword);
  tweetCount = 0;
  duplicateCount = 0;
  createdCount = 0;

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

  // Use buffering because data from stdout will be chunked
  let stdoutBuffer = "";
  childProcess.stdout.on("data", (data: any) => {
    // If no newline is found, add data to buffer
    if (!data.toString().includes("\n")) {
      stdoutBuffer += data.toString();
      return;
    }

    // Consume buffer and append newest data
    const lines = (stdoutBuffer + data.toString()).split("\n");
    stdoutBuffer = "";

    const parsed = [];
    for (let line of lines) {
      if (line === "") continue;
      try {
        const tweet = JSON.parse(line);
        tweet.coin = coin;
        parsed.push(tweet);
      } catch (err: any) {
        if (err.toString().includes("Unexpected end of JSON input")) {
          stdoutBuffer = line;
          continue;
        }
        console.log(err);
      }
    }

    chunk.push(...parsed);
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
      createdCount++;

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
// setInterval(async () => {
//   if (duplicateCount >= currentIntervalCount) {
//     await handleDuplicateCount().then(() => {
//       process.exit();
//     });
//   }
// }, 600000);

let oldTweetCount = 0;
function wait() {
  return new Promise((resolve) => {
    const i = setInterval(() => {
      // Check if we got new tweets
      if (oldTweetCount != 0 && oldTweetCount === tweetCount) {
        clearInterval(i);
        return resolve(ContinueReason.NO_NEW_TWEETS);
      } else {
        oldTweetCount = tweetCount;
      }
      // Check duplicates
      if (duplicateCount / tweetCount > 0.2) {
        clearInterval(i);
        resolve(ContinueReason.TOO_MANY_DUPLICATES);
      }
    }, 1000 * 60 * 10);
  });
}

async function sendNotification(msg: string) {
  const statusEmbed = buildStatusEmbed(msg);
  await hook.send(statusEmbed);
}

function buildStatusEmbed(description: string) {
  return new MessageBuilder()
    .setTitle("Status Update")
    .setDescription(description)
    .addField("📜Current Keyword", keyword, true)
    .addField("⚙️ Tweets Processed:", tweetCount.toString(), true)
    .addField("⚠️ Duplicates:", duplicateCount.toString(), true)
    .addField("✅Created", createdCount.toString(), true)
    .setColor(1)
    .setFooter(`${coin}`)
    .setTimestamp();
}
