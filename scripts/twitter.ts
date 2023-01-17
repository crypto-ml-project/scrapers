import { PrismaClient } from "@prisma/client";
import { spawn } from "child_process";
import { MessageBuilder, Webhook } from "discord-webhook-node";
import fs from "fs-extra";

const prisma = new PrismaClient();
const hook = new Webhook(
  "https://discord.com/api/webhooks/1057807224182931556/XIvU0Mi7yKGGhBu7gJH7sYAgn68Jo7fLSVorots5m7CdzCquHCR03sVsz--uA6KxDNkU"
);

const platform = "twitter";
const coin = "optimism";

const child = spawn("snscrape", [
  "-n 100000",
  "--jsonl",
  "twitter-search",
  "Optimism Crypto",
]);

child.stderr.on("data", (data: any) => {
  console.error(`stderr: ${data}`);
  fs.appendFile("errors.txt", `${new Date()} ${data}`);
  hook.error("**Error Processing Tweets:**", `${data}`);
});

let tweetCount = 0;
let duplicateCount = 0;
let currentIntervalCount = 0;
let totalIntervalCount = 0;

let chunk: any[] = [];
let saving = false;

child.stdout.on("data", async (data: any) => {
  let tmpTweets = data.toString().split(/\n/g);
  tmpTweets = tmpTweets.filter((tweet: any) => tweet !== "");

  const tweets = tmpTweets.slice(0, tmpTweets.length - 1).map((tweet: any) => {
    return JSON.parse(tweet);
  });

  chunk.push(...tweets);
});

async function saveChunk() {
  saving = true;

  try {
    const txs: any = [];
    tweetCount += chunk.length;

    chunk.forEach(async (tweet: any) => {
      const postId = `${platform}_${tweet.url.split("/")[5]}_${coin}`;

      const postCheck = await prisma.socialMediaPosts.findUnique({
        where: {
          id: postId,
        },
      });

      if (postCheck) {
        duplicateCount++;
        return;
      }

      currentIntervalCount++;
      totalIntervalCount++;

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
        platformId: platform,
        tags: tweet.hashtags ? tweet.hashtags : [],
        userId: tweet.user.username || "",
        coin: coin,
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

    chunk = [];

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
    totalIntervalCount;
  const date = new Date();
  console.log(date.toLocaleDateString(), date.toLocaleTimeString(), msg);
}, 1000);

setInterval(async () => {
  if (duplicateCount >= currentIntervalCount) {
    await handleDuplicateCount().then(() => {
      process.exit();
    });
  }
}, 600000);

async function handleDuplicateCount() {
  const msg =
    "No new tweets found for the past 10min's. Moving to the next Keyword.";
  const statusEmbed = buildStatusEmbed({
    description:
      "No new tweets found for the past 10min's. Moving to the next Keyword.",
    tweetCount: tweetCount.toLocaleString(),
    duplicateCount: duplicateCount.toLocaleString(),
    intervalCount: totalIntervalCount.toLocaleString(),
  });
  await hook.send(statusEmbed);
  currentIntervalCount = 0;
  duplicateCount = 0;
  console.log(msg);
  return;
}

function buildStatusEmbed({
  description,
  tweetCount,
  duplicateCount,
  intervalCount,
}: {
  description: string;
  tweetCount: string;
  duplicateCount: string;
  intervalCount: string;
}) {
  return new MessageBuilder()
    .setTitle("Status Update")
    .setDescription(description)
    .addField("⚙️ Tweets Processed:", `${tweetCount}`, true)
    .addField("⚠️ Duplicates:", `${duplicateCount}`, true)
    .addField("✅ Created", `${intervalCount}`, true)
    .setColor(1)
    .setFooter(`${coin}`)
    .setTimestamp();
}
