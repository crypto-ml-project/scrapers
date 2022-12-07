import { PrismaClient } from "@prisma/client";
import { MessageBuilder, Webhook } from "discord-webhook-node";
const { spawn } = require("child_process");
const fs = require("fs-extra");

const prisma = new PrismaClient();
const hook = new Webhook(
  "https://discord.com/api/webhooks/1049743519356571679/QF89ZzJFbpEOwoYRTgjLaqoAmwJWu6nabYxNQyOquc32NMx1MP8eqAL7Iu_SQNABaw8E"
);
const child = spawn("snscrape", [
  "-n 1000000",
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
let createCount = 0;
let duplicateCount = 0;

child.stdout.on("data", async (data: any) => {
  let tmpTweets = data.toString().split(/\n/g);

  tmpTweets = tmpTweets.filter((tweet: any) => tweet !== "");

  const tweets = tmpTweets.slice(0, tmpTweets.length - 1).map((tweet: any) => {
    tweetCount++;
    return JSON.parse(tweet);
  });

  for (const tweet of tweets) {
    const user = await addUserData(tweet);
    const post = await addPostData(tweet);
    if (!user || !post) {
      duplicateCount++;
    } else {
      createCount++;
    }
  }
});

setInterval(async () => {
  const msg = "⚙️ Tweets Processed: " + tweetCount;
  const createMsg = "✅ Tweets Created: " + createCount;
  const duplicateMsg = "⚠️ Duplicate Tweets: " + duplicateCount;
  const date = new Date();
  console.log(
    date.toLocaleDateString(),
    date.toLocaleTimeString(),
    msg,
    createMsg,
    duplicateMsg
  );
}, 1000);

setInterval(async () => {
  const msg = "⚙️ Tweets Processed: " + tweetCount;
  const createMsg = "✅ Tweets Created: " + createCount;
  const duplicateMsg = "⚠️ Duplicate Tweets: " + duplicateCount;
  const date = new Date();
  hook.send(
    `\`\`\`${date.toLocaleDateString()} ${date.toLocaleTimeString()} | ${msg} | ${createMsg} | ${duplicateMsg}\`\`\``
  );
  if (
    tweetCount > 10000 &&
    (duplicateCount > createCount * 0.9 || createCount === 0)
  ) {
    hook.warning("**Duplicate Tweets:**", duplicateCount.toLocaleString());
  }
}, 1.8e6);

async function addUserData(tweet: any) {
  const usercheck = await prisma.user.findUnique({
    where: {
      userId: tweet.user.username,
    },
  });

  if (usercheck) {
    // console.log("⚠️ User Data already exists for: " + tweet.user.username);
    return false;
  } else {
    const user = await prisma.user.create({
      data: {
        userId: tweet.user.username,
        created_at: tweet.user.created,
        follower: tweet.user.followersCount,
        description: tweet.user.description,
      },
    });

    if (!user) {
      hook.error("**❌ Failed to populate User Data:**", tweet.user.username);
      return false;
    }
    return true;
  }
}

async function addPostData(tweet: any) {
  const tweetId = tweet.url.split("/")[5];
  const postCheck = await prisma.socialMediaPosts.findUnique({
    where: {
      id: tweetId,
    },
  });

  if (postCheck) {
    return false;
  } else {
    const socialMediaPost = await prisma.socialMediaPosts.create({
      data: {
        id: tweet.url.split("/")[5],
        content: tweet.content,
        timeStamp: tweet.date,
        like: tweet.likeCount,
        comments: tweet.replyCount,
        platformId: "twitter",
        tags: tweet.hashtags ? tweet.hashtags : [],
        user: {
          connect: {
            userId: tweet.user.username,
          },
        },
      },
    });

    if (!socialMediaPost) {
      hook.error("**❌ Failed to populate Social Media Post Data:**", tweetId);
      return false;
    }
    return true;
  }
}
