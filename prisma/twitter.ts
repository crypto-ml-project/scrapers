import { PrismaClient } from "@prisma/client";
import { Webhook } from "discord-webhook-node";

const hook = new Webhook(
  "https://discord.com/api/webhooks/1049743519356571679/QF89ZzJFbpEOwoYRTgjLaqoAmwJWu6nabYxNQyOquc32NMx1MP8eqAL7Iu_SQNABaw8E"
);

const { spawn } = require("child_process");
const fs = require("fs-extra");
const child = spawn("snscrape", [
  "-n 1000000",
  "--jsonl",
  "twitter-search",
  "Optimism Crypto",
]);
const jsonLog = fs.createWriteStream("twitter.json", { flags: "a" });

child.stdout.pipe(jsonLog);

const prisma = new PrismaClient();

child.stderr.on("data", (data: any) => {
  console.error(`stderr: ${data}`);
  fs.appendFile("errors.txt", `${new Date()} ${data}`);
});

let tweetCount = 0;

child.stdout.on("data", async (data: any) => {
  const match = data.toString().match(/\n/g);
  tweetCount += match?.length ? match.length : 0;
});

setInterval(async () => {
  const msg = "Tweets Processed: " + tweetCount;
  console.log(new Date(), msg);
}, 1000);


setInterval(async () => {
  const msg = "Tweets Processed: " + tweetCount;
  hook.send(msg);
}, 1.8e6);

child.on("close", async (code: any) => {
  console.log(`child process exited with code ${code}`);
  console.log("⚙️ Populating Db with Twitter Data");
  const fileData = await fs.readFile("twitter.json", { flags: "a" });
  const data = fileData.toString().split("\n");
  const tweets = data.slice(0, data.length - 1).map((tweet: any) => {
    return JSON.parse(tweet);
  });

  for (const tweet of tweets) {
    await addUserData(tweet);
    await addPostData(tweet);
  }

  console.log("✅ Populated Db with Twitter Data");
});

async function addUserData(tweet: any) {
  console.log("⚙️ Populating User Data");
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
