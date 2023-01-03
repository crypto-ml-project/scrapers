import { PrismaClient } from "@prisma/client";

const data = require("../data/discord/lido.json");

const prisma = new PrismaClient();

const main = async function () {
  const authors = data.messages.map((message: any) => message.author);
  const createUser = await prisma.user.createMany({
    data: authors.map((author: any) => {
      return {
        userId: author.name + "#" + author.discriminator,
        created_at: new Date(),
        follower: 0,
        description: "",
      };
    }),
    skipDuplicates: true,
  });

  const createData = await prisma.socialMediaPosts.createMany({
    data: data.messages.map((message: any) => {
      return {
        id: "discord_" + message.id + "_" + data.guild.name.toLowerCase(),
        content: message.content,
        timeStamp: new Date(message.timestamp),
        like:
          message.reactions?.reduce(function (tot: any, arr: any) {
            return tot + arr.count;
          }, 0) ?? 0,
        comments: 0,
        platformId: "discord",
        tags: [],
        coin: data.guild.name.toLowerCase(),
        userId: message.author.name + "#" + message.author.discriminator,
      };
    }),
    skipDuplicates: true,
  });
};

main();
