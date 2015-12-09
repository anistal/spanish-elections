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
import scala.io.Source
import scala.util.{Failure, Success, Try}

object TwitterElectionRunner {

  private val logger = Logger.getLogger(this.getClass)
  private val Empty = ""
  private val MinRetweetCount = 10
  implicit val formats: Formats = DefaultFormats

  var areas: Map[String, String] = Map()

  var producer: Producer[String, String] = _

  private val twitterListener = new StatusListener() {
    def onStatus(status: Status) {
      processStatus(status)
    }

    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}

    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}

    def onException(ex: Exception) {
      logger.error(ex.getLocalizedMessage, ex)
    }

    def onScrubGeo(arg0: Long, arg1: Long) {}

    def onStallWarning(warning: StallWarning) {}
  }

  def main(args: Array[String]) = {
    if (args.size == 0) {
      logger.info("Use: java -jar twitter-election.jar <config file>")
      System.exit(1)
    }
    Try(ConfigFactory.parseFile(new File(args(0)))) match {
      case Success(config) =>
        initializeAreas()
        start(config)
      case Failure(exception) => {
        logger.error(exception.getLocalizedMessage, exception)
        System.exit(1)
      }
    }
  }

  def initializeAreas(): Unit = {
    val areasLines = Source.fromInputStream(this.getClass.getClassLoader
      .getResourceAsStream("areas.txt")).getLines().toSeq
    areas = areasLines.map(line => line.split(",")(0) -> line.split(",")(1)).toMap
  }

  def start(config: Config): Unit = {
    producer = KafkaProducer.getInstance(config)
    val twitterStream = new TwitterStreamFactory(getTwitterConfig(config)).getInstance
    twitterStream.addListener(twitterListener)
    twitterStream.filter(config.getStringList("trackWords").asScala.mkString(","))
  }

  def processStatus(status: Status): Unit = {
    logger.info(s">> ${status.getText}")
    val event = generateEvent(status)
    generateHashtags(status, event)
    generateUserMentions(status, event)
    generateUrl(status, event)
    generateMediaEntities(status, event)
    generateRetweets(status, event)
  }

  // XXX

  def generateRetweets(status: Status, event: TwitterModel) = {
    if (event.retweetCount >= MinRetweetCount && event.retweetId != 0)
      KafkaProducer.put(producer, "retweets", write(event))
  }

  def generateHashtags(status: Status, event: TwitterModel) = {
    status.getHashtagEntities.map(hashtagEntity => {
      val twitterModel = event.copy(hashtag = hashtagEntity.getText)
      KafkaProducer.put(producer, "hashtags", write(twitterModel))
      KafkaProducer.put(producer, "influenceHashtags", write(twitterModel))
    })
  }

  def generateUserMentions(status: Status, event: TwitterModel) = {
    status.getUserMentionEntities.map(userMentionEntity => {
      val twitterModel = event.copy(userMention = userMentionEntity.getScreenName)
      KafkaProducer.put(producer, "usermentions", write(twitterModel))
      KafkaProducer.put(producer, "influenceMentions", write(twitterModel))
    })
  }

  def generateUrl(status: Status, event: TwitterModel) = {
    status.getURLEntities.map(uRLEntity => {
      val twitterModel = event.copy(url = uRLEntity.getURL)
      KafkaProducer.put(producer, "urls", write(twitterModel))
    })
  }

  def generateMediaEntities(status: Status, event: TwitterModel) = {
    status.getMediaEntities.map(mediaEntity => {
      val twitterModel = event.copy(media = mediaEntity.getMediaURL)
      KafkaProducer.put(producer, "mediaentities", write(twitterModel))
    })
  }

  // XXX

  def generateEvent(status: Status): TwitterModel = {
    val id = status.getId
    val createdAt = status.getCreatedAt.getTime
    val text = status.getText



    val currentUserRetweetId = status.getCurrentUserRetweetId
    val user = if (status.getCurrentUserRetweetId == -1) status.getUser.getScreenName else ""
    val userId = if (status.getCurrentUserRetweetId == -1) status.getUser.getId else status.getCurrentUserRetweetId

    val location = if (status.getUser.getLocation != null) {
      val filterAreas = areas.filter(area => status.getUser.getLocation.toLowerCase.contains(area._1))
      if (filterAreas.size != 0) filterAreas.head._2 else "No location"
    } else "No location"

    val followersCount = status.getUser.getFollowersCount

    val retweetId = if(status.getRetweetedStatus != null) status.getRetweetedStatus.getId else 0
    val retweetCount = if(status.getRetweetedStatus != null) status.getRetweetedStatus.getRetweetCount else 0

    TwitterModel(id, createdAt, text, Empty, Empty, Empty, Empty, currentUserRetweetId, user, userId, location,
      followersCount, retweetId, retweetCount)
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
}
