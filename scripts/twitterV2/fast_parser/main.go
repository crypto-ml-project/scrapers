package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/valyala/fastjson"
)

type User struct {
	id string
	name string
	username string
	follower int
	description string
}

type Tweet struct {
	id string
	content string
	timeStamp string
	like int
	comments int
	platformId string
	tags []string
	userId string
	coin string
	keyword string
	lang string
}

func exists(id int, list []int) bool {
	for _, i := range list {
		if i == id {
			return true
		}
	}
	return false
}

func escape(s string) string{
	e := strings.ReplaceAll(s, "\"", "\"\"")
	e = strings.ReplaceAll(e, "\n", " ")
	return e
}

func main () {
	var users []User
	var tweets []Tweet

	var knownUsers []int
	var knownTweets []int

	var count int
	var lineCount int
	var errors int

	var p fastjson.Parser

	readFile, err := os.Open("../output.txt")
	if err != nil {
		fmt.Println(err)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		lineCount++

		line := fileScanner.Text()
		v, err := p.Parse(line)
		if err != nil {
			if !strings.Contains(err.Error(), "tail") {
				fmt.Println(err)
			}
			errors++
			continue
		}

		if string(v.GetStringBytes("type")) == "tweet" {
			id := v.GetInt("data", "id")
			if exists(id, knownTweets) {
				continue
			}
			var tags []string
			i := 0
			for {
				value := v.GetObject("data", "tags", strconv.Itoa(i))
				if value == nil {
					break
				}
				tags = append(tags, value.Get("name").String())
				i++
			}
			tweets = append(tweets, Tweet{
				id: string(v.GetStringBytes("data", "id_str")),
				content: string(v.GetStringBytes("data", "full_text")),
				timeStamp: string(v.GetStringBytes("data", "created_at")),
				like: int(v.GetInt("data", "favorite_count")),
				comments: int(v.GetInt("data", "reply_count")),
				platformId: "twitter",
				tags: tags,
				userId: string(v.GetStringBytes("data", "user_id_str")),
				keyword: string(v.GetStringBytes("keyword")),
				coin: string(v.GetStringBytes("coin")),
				lang: string(v.GetStringBytes("data", "lang")),
			})
			knownTweets = append(knownTweets, id)
			count++
			if count % 10_000 == 0 {
				fmt.Println("line:", lineCount, "tweets:",count, "errors:", errors)
			}
		} else if string(v.GetStringBytes("type")) == "user" {
			id := v.GetInt("data", "id")
			if exists(id, knownUsers) {
				continue
			}
			users = append(users, User{
				id: string(v.GetStringBytes("data", "id_str")),
				name: string(v.GetStringBytes("data", "name")),
				username: string(v.GetStringBytes("data", "screen_name")),
				follower: int(v.GetInt("data", "followers_count")),
				description: string(v.GetStringBytes("data", "description")),
			})
			knownUsers = append(knownUsers, id)
		}
	}
	readFile.Close()

	tweetsOut, err := os.OpenFile("./tweets.csv", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	tweetsOut.WriteString("id,content,timeStamp,like,comments,platformId,tags,userId,coin,keyword,lang\n")
	for i, tweet := range tweets {
		if i % 10_000 == 0 {
			fmt.Println("wrote tweets:", i)
		}
		content := escape(tweet.content)
		tweetsOut.WriteString(fmt.Sprintf(`"%s","%s","%s",%d,%d,"%s","%s","%s","%s","%s","%s"` + "\n", tweet.id, content, tweet.timeStamp, tweet.like, tweet.comments, tweet.platformId, tweet.tags, tweet.userId, tweet.coin, tweet.keyword, tweet.lang))
	}
	tweetsOut.Close()

	usersOut, err := os.OpenFile("./users.csv", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	usersOut.WriteString("id,name,username,follower,description\n")
	for i, user := range users {
		if i % 10_000 == 0 {
			fmt.Println("wrote users:", i)
		}
		description := escape(user.description)
		username := escape(user.username)
		name := escape(user.name)
		usersOut.WriteString(fmt.Sprintf(`"%s","%s","%s",%d,"%s"` + "\n", user.id, name, username, user.follower, description))
	}
	usersOut.Close()

}
