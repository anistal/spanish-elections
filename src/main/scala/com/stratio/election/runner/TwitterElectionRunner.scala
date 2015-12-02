package com.stratio.election.runner

import java.io.File

import com.stratio.election.kafka.KafkaProducer
import com.stratio.election.model.TwitterModel
import com.typesafe.config.{Config, ConfigFactory}
import kafka.producer.Producer
import twitter4j._
import twitter4j.conf.{Configuration, ConfigurationBuilder}
import org.json4s.native.Serialization._
import org.json4s.{DefaultFormats, Formats}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object TwitterElectionRunner {

  private val logger = Logger.getLogger(this.getClass)
  private val KafkaTopic: String = "election"
  private val userMention = ""
  private val hashtag = ""
  private val url = ""
  private val media = ""
  private val currentUserRetweetId = ""
  private val user = ""
  private val locationFirstLevel = ""
  private val locationSecondLevel = ""
  private val followersCount = 0

  implicit val formats: Formats = DefaultFormats

  var producer: Producer[String,String] = _

  private val twitterListener = new StatusListener() {
    def onStatus(status: Status) {
      processStatus(status)
    }
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
    def onException(ex: Exception) { logger.error(ex.getLocalizedMessage, ex) }
    def onScrubGeo(arg0: Long, arg1: Long) {}
    def onStallWarning(warning: StallWarning) {}
  }

  def main(args: Array[String]) = {
    if(args.size == 0) {
      logger.info("Use: java -jar twitter-election.jar <config file>")
      System.exit(1)
    }
   Try(ConfigFactory.parseFile(new File(args(0)))) match {
      case Success(config) => start(config)
      case Failure(exception) => {
        logger.error(exception.getLocalizedMessage, exception)
        System.exit(1)
      }
    }
  }

  def start(config: Config): Unit = {
    producer = KafkaProducer.getInstance(config)
    val twitterStream = new TwitterStreamFactory(getTwitterConfig(config)).getInstance
    twitterStream.addListener(twitterListener)
    twitterStream.filter(config.getStringList("trackWords").asScala.mkString(","))
  }

  def processStatus(status: Status): Unit = {
    logger.info(status.getText)
    generateUnique(status)
    generateHashtags(status)
    generateUserMentions(status)
    generateUrl(status)
    generateMediaEntities(status)
  }

  def getTwitterConfig(config: Config): Configuration = {
    val consumerKey = config.getString("consumerKey")
    val consumerSecret = config.getString("consumerSecret")
    val accessToken = config.getString("accessToken")
    val accessTokenSecret = config.getString("accessTokenSecret")

    new ConfigurationBuilder()
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .build
  }

  def generateUnique(status: Status) = {
    val id = status.getId
    val createdAt = status.getCreatedAt.getTime
    val text = status.getText
    val unique = "true"

    val twitterModel = TwitterModel(id, createdAt, text, userMention, hashtag, url, media, currentUserRetweetId,
      user, locationFirstLevel, locationSecondLevel, followersCount, unique)

    KafkaProducer.put(producer, KafkaTopic ,write(twitterModel))
  }

  def generateHashtags(status: Status) = {
    val id = status.getId
    val createdAt = status.getCreatedAt.getTime
    val text = status.getText
    val unique = "false"

    status.getHashtagEntities.map(hashtagEntity => {
      val twitterModel = TwitterModel(id, createdAt, text, userMention, hashtagEntity.getText, url, media,
        currentUserRetweetId,
        user, locationFirstLevel, locationSecondLevel, followersCount, unique)
      KafkaProducer.put(producer, KafkaTopic ,write(twitterModel))
    })
  }

  def generateUserMentions(status: Status) = {
    val id = status.getId
    val createdAt = status.getCreatedAt.getTime
    val text = status.getText
    val unique = "false"
    status.getUserMentionEntities.map(userMentionEntity => {
      val twitterModel = TwitterModel(id, createdAt, text, userMentionEntity.getScreenName, hashtag, url, media,
        currentUserRetweetId,
        user, locationFirstLevel, locationSecondLevel, followersCount, unique)
      KafkaProducer.put(producer, KafkaTopic ,write(twitterModel))
    })
  }

  def generateUrl(status: Status) = {
    val id = status.getId
    val createdAt = status.getCreatedAt.getTime
    val text = status.getText
    val unique = "false"
    status.getURLEntities.map(uRLEntity => {
      val twitterModel = TwitterModel(id, createdAt, text, userMention, hashtag, uRLEntity.getExpandedURL, media,
        currentUserRetweetId,
        user, locationFirstLevel, locationSecondLevel, followersCount, unique)
      KafkaProducer.put(producer, KafkaTopic , write(twitterModel))
    })
  }

  def generateMediaEntities(status: Status) = {
    val id = status.getId
    val createdAt = status.getCreatedAt.getTime
    val text = status.getText
    val unique = "false"
    status.getMediaEntities.map(mediaEntity => {
      val twitterModel = TwitterModel(id, createdAt, text, userMention, hashtag, url, mediaEntity.getExpandedURL,
        currentUserRetweetId,
        user, locationFirstLevel, locationSecondLevel, followersCount, unique)
      KafkaProducer.put(producer, KafkaTopic ,write(twitterModel))
    })
  }
}
