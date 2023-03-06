const https = require("https");
const fs = require("fs");

const cursors = JSON.parse(fs.readFileSync("cursors.json"));
let buffer = [];

setInterval(() => {
	const data = buffer.map(b => JSON.stringify(b)).join("\n");
	buffer = [];
	fs.appendFile("output.txt", data, {}, () => {});
}, 1000);

async function newSession() {
	console.log("new session...");
	const ua = `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.${(
		Math.random() * 9999
	).toFixed(0)} Safari/537.${(Math.random() * 99).toFixed(0)}`;

	const data = await new Promise((resolve, reject) => {
		https.get("https://twitter.com", {
			headers: {
				"User-Agent": ua,
			},
		}, (response) => {
			let data;
			response.on("data", (chunk) => data += chunk);
			response.on("end", () => resolve(data));
			response.on("error", (err) => reject(err));
		});
	})

	const token = data.match(/\"gt=(\d+);/)[1];

	const tls = 'TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-RSA-AES128-SHA:ECDHE-RSA-AES256-SHA:AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA:AES256-SHA';

	return {
		headers: {
			"User-Agent": ua,
			Referer: "https://twitter.com",
			"X-GUEST-TOKEN": token,
			Authorization: "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs=1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
			"Accept-Language": "en-US,en;q=0.5",
		},
		ciphers: tls
	};
}

function handler(data, keyword, session) {
	const instructions = data?.timeline?.instructions;
	if (!instructions) {
		console.log("no instructions");
	}
	// Parse instructions
	for (let instruction of instructions) {
		// Filter for entries
		let entries = instruction?.addEntries?.entries || [
			instruction?.replaceEntry?.entry,
		];
		if (!entries || !entries?.[0]) continue;

		// Parse entries
		for (let entry of entries) {
			// Set next cursor
			if (entry.entryId === "sq-cursor-bottom") {
				// Find cursor for next page
				const cursor = entry.content.operation.cursor.value;
				cursors[keyword] = cursor;
				console.log(keyword, "next page:", cursor);

				// Update cursors cache JSON file
				fs.writeFile("cursors.json", JSON.stringify(cursors), {}, () => { });

				search(keyword, session);
				continue;
			}
			const tweet_id = entry?.content?.item?.content?.tweet?.id;
			if (!tweet_id) continue;
			const tweet = data.globalObjects.tweets[tweet_id];
			if (!tweet) continue;
			const userId = tweet.user_id || tweet.user_id_str;
			const user = data.globalObjects.users[userId];
			if (!user) continue;
			//console.log(JSON.stringify(tweet), JSON.stringify(userId));
			buffer.push({
				keyword,
				type: "tweet",
				data: tweet
			});
			buffer.push({
				keyword,
				type: "user",
				data: user
			});
		}
	}
}

async function search(keyword, session) {
	const search_opts = {
		include_profile_interstitial_type: 1,
		include_blocking: "1",
		include_blocked_by: "1",
		include_followed_by: "1",
		include_want_retweets: "1",
		include_mute_edge: "1",
		include_can_dm: "1",
		include_can_media_tag: "1",
		include_ext_has_nft_avatar: "1",
		include_ext_is_blue_verified: "1",
		include_ext_verified_type: "1",
		skip_status: "1",
		cards_platform: "Web-12",
		include_cards: "1",
		include_ext_alt_text: "true",
		include_ext_limited_action_results: "false",
		include_quote_count: "true",
		include_reply_count: "1",
		tweet_mode: "extended",
		include_ext_collab_control: "true",
		include_ext_views: "true",
		include_entities: "true",
		include_user_entities: "true",
		include_ext_media_color: "true",
		include_ext_media_availability: "true",
		include_ext_sensitive_media_warning: "true",
		include_ext_trusted_friends_metadata: "true",
		send_error_codes: "true",
		simple_quoted_tweet: "true",
		count: "20",
		query_source: "spelling_expansion_revert_click",
		pc: "1",
		spelling_corrections: "1",
		include_ext_edit_control: "true",
		ext: "mediaStats,highlightedLabel,hasNftAvatar,voiceInfo,enrichments,superFollowMetadata,unmentionInfo,editControl,collab_control,vibe",
		tweet_search_mode: "live",
		q: keyword
	};

	if (cursors[keyword]) {
		search_opts.cursor = cursors[keyword];
	}

	if (!session) {
		session = await newSession();
	}
	const url = `https://api.twitter.com/2/search/adaptive.json?${new URLSearchParams(search_opts)}`;

	const RETRY_DELAY = 1000;
	https.get(url, {
		...session
	}, (res) => {
		const retry = () => {
			console.log(res.statusCode, "retry");
			setTimeout(() => {
				search(keyword, null);
			}, RETRY_DELAY);
		};
		let data = "";
		res.on("data", (chunk) => data += chunk);
		res.on("end", () => {
			if (res.statusCode !== 200) {
				retry();
				return;
			}
			try {
				const json = JSON.parse(data);
				handler(json, keyword, session);
			} catch (err) {
				console.log(err);
				retry();
				return;
			}
		});
	});
}

search("Bitcoin", null);
search("Solana", null);
