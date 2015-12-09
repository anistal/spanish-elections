package com.stratio.election.model

import java.util.Date

case class TwitterModel(idTweet: Long = 0L,
                        createdAt: Long = new Date().getTime,
                        text: String = "No text",
                        userMention: String = "No mention",
                        hashtag: String = "No hashtag",
                        url: String = "No url",
                        media: String = "No media",
                        currentUserRetweetId: Long = 0L,
                        user: String = "No user",
                        userId: Long = 0L,
                        location: String = "No location",
                        followersCount: Int = 0,
                        retweetId: Long = 0L,
                        retweetCount: Int = 0) {}