const fs = require("fs");
const readline = require("readline");

function main() {
	const known = [];
	let duplicateTweets = 0;
	let duplicateUsers = 0;

	const users = [];
	const tweets = [];
	let counter = 0;

	const readStream = fs.createReadStream('output.txt', 'utf-8');
	const rl = readline.createInterface({ input: readStream })
	rl.on('line', (line) => {
		counter++;
		if(counter % 10_000 === 0){
			console.log("@", counter);
		}
		let entry;
		try{
			entry = JSON.parse(line);
		}catch(err){
			// Workaround for invalid file appends
			const match = err.message.match(/Unexpected token { in JSON at position (\d+)/);
			if(match?.length){
				entry = JSON.parse(line.slice(0, parseInt(match[1])));
			}else{
				console.log("error parsing line:", err, "msg:", line);
				return;
			}
		}
		const { coin, keyword, type, data } = entry;
		if (type === "tweet") {
			if (known.includes(data.id_str)) {
				duplicateTweets++;
				return;
			}
			tweets.push({
				id: data.id_str,
				content: data.full_text,
				timeStamp: data.created_at,
				like: data.favorite_count,
				comments: data.reply_count,
				platformId: "twitter",
				tags: data.entities.hashtags.map((tag) => tag.text),
				userId: data.user_id_str,
				coin
			});
			known.push(data.id_str);
		}
		if (type === "user") {
			if (known.includes(data.id_str)) {
				duplicateUsers++;
				return;
			}
			users.push({
				id: data.id_str,
				name: data.name,
				username: data.screen_name,
				follower: data.followers_count,
				description: data.description,
			});
			known.push(data.id_str);
		}
	});
	rl.on('error', (error) => console.log(error.message));
	rl.on('close', () => {
		console.log(tweets.length, users.length);
	})
}
main();
