package com.stratio.election.model

case class TwitterModel(id: Long,
                        createdAt: Long,
                        text: String,
                        userMention: String,
                        hashtag: String,
                        url: String,
                        media: String,
                        currentUserRetweetId: Long,
                        user: String,
                        locationFirstLevel: String,
                        locationSecondLevel: String,
                        followersCount: Int,
                        unique: String) {}