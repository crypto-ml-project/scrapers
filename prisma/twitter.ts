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
let duplicateCount = 0;

child.stdout.on("data", async (data: any) => {
  const tweetsLength = data.toString().match(/\n/g).length;
  tweetCount += tweetsLength;

  let tmpTweets = data.toString().split(/\n/g);

  tmpTweets = tmpTweets.filter((tweet: any) => tweet !== "");

  const tweets = tmpTweets.slice(0, tmpTweets.length - 1).map((tweet: any) => {
    return JSON.parse(tweet);
  });

  for (const tweet of tweets) {
    await addUserData(tweet);
    await addPostData(tweet);
  }
});

setInterval(async () => {
  const msg = "Tweets Processed: " + tweetCount;
  console.log(tweetCount, duplicateCount);

  console.log(new Date().toDateString(), msg);
}, 1000);

setInterval(async () => {
  hook.info("**Tweets Processed:**", tweetCount.toLocaleString());
  if (tweetCount > 10000 && duplicateCount > tweetCount * 0.75) {
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
    console.log("⚠️ User Data already exists for: " + tweet.user.username);
    return;
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
      console.log("❌ Failed to populate User Data");
      return false;
    }

    console.log("✅ Populated User Data for: " + tweet.user.username);
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
    duplicateCount++;
    console.log("⚠️ Post Data already exists for: " + tweetId);
    return;
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
      console.log("❌ Failed to populate Social Media Post Data");
      return false;
    } else {
      console.log(
        "✅ Populated Social Media Post Data from: " + socialMediaPost.userId
      );
      return true;
    }
  }
}
