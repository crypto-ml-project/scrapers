import { PrismaClient } from "@prisma/client";
import { Webhook } from "discord-webhook-node";
import { spawn } from "child_process";
import fs from "fs-extra";

const prisma = new PrismaClient();
const hook = new Webhook(
  "https://discord.com/api/webhooks/1049743519356571679/QF89ZzJFbpEOwoYRTgjLaqoAmwJWu6nabYxNQyOquc32NMx1MP8eqAL7Iu_SQNABaw8E"
);

const coin = "optimism";

const child = spawn("snscrape", [
  "-n 10000000",
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
let intervalCount = 0;

let chunk: any[] = [];
let saving = false;

child.stdout.on("data", async (data: any) => {
  let tmpTweets = data.toString().split(/\n/g);
  tmpTweets = tmpTweets.filter((tweet: any) => tweet !== "");

  const tweets = tmpTweets.slice(0, tmpTweets.length - 1).map((tweet: any) => {
    return JSON.parse(tweet);
  });

  tweetCount += tweets.length;

  chunk.push(...tweets);
});

async function saveChunk() {
  saving = true;

  try {
    const txs: any = [];

    for (let tweet of chunk) {
      intervalCount++;

      const postCheck = await prisma.socialMediaPosts.findUnique({
        where: {
          id: tweet.url.split("/")[5],
        },
      });

      if (postCheck) {
        duplicateCount++;
      }

      const userData = {
        userId: tweet.user.username,
        created_at: tweet.user.created,
        follower: tweet.user.followersCount,
        description: tweet.user.description,
      };

      const tx = prisma.user.upsert({
        where: {
          userId: userData.userId,
        },
        create: userData,
        update: userData,
      });

      txs.push(tx);

      const tweetData = {
        id: tweet.url.split("/")[5],
        content: tweet.content,
        timeStamp: tweet.date,
        like: tweet.likeCount,
        comments: tweet.replyCount,
        platformId: "twitter",
        tags: tweet.hashtags ? tweet.hashtags : [],
        userId: tweet.user.username,
        coin: coin,
      };

      const tx2 = prisma.socialMediaPosts.upsert({
        where: {
          id: tweetData.id,
        },
        create: tweetData,
        update: tweetData,
      });

      txs.push(tx2);
    }

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
  const msg = "⚙️ Tweets Processed: " + tweetCount;
  const date = new Date();
  console.log(date.toLocaleDateString(), date.toLocaleTimeString(), msg);
}, 1000);

setInterval(async () => {
  if (intervalCount === duplicateCount) {
    const date = new Date();
    console.log("No new tweets found", intervalCount, duplicateCount);
    hook.warning(
      `\`\`\`${date.toLocaleDateString()} | Only able to find duplicate Tweets for the past 10min's. Moving to the next Keyword.\`\`\``
    );
  }
}, 600000);

setInterval(async () => {
  const msg = "⚙️ Tweets Processed: " + tweetCount;
  const date = new Date();
  hook.send(
    `\`\`\`${date.toLocaleDateString()} ${date.toLocaleTimeString()} | ${msg}\`\`\``
  );
}, 1.8e6);
