import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.io.Source
import scala.math.pow
import scala.concurrent.duration._

class DataPoint(val Features: Array[Double], val Centroid: Int, val Label: Int)

class Centroid(val Name: Int, val Features: Array[Double])

class CentroidSuggestion(val Centroid: Int, val Features: Array[Double], val Count: Int)

object KmeansActor {
  case object Cluster
  case object GetNewCentroids
  case object GetDataPoints
  final case class UpdateCentroids(newCentroids: Array[Centroid])
  case object UpdateCentroids
}

class KmeansActor(dataPartition: Array[DataPoint], initialCentroids: Array[Centroid]) extends Actor {
  import KmeansActor._

  var data = dataPartition
  var centroids = initialCentroids
  val hash = this.hashCode()

  def updateDatapoint(dataPoint: DataPoint, newCentroid: Centroid) = {
    new DataPoint(dataPoint.Features, newCentroid.Name, dataPoint.Label)
  }

  def updateCentroidSuggestion(centroid: Int, newFeatures: Array[Double], newCount: Int) = {
    new CentroidSuggestion(centroid, newFeatures, newCount)
  }

  def receive = {
    case Cluster =>
      // println("Doing clustering " + hash)

      val oldCentroids = data map(x => x.Centroid)

      data = data map(dataPoint =>
          updateDatapoint(dataPoint, centroids minBy(centroid =>
              ((centroid.Features zip dataPoint.Features) map {case (f1, f2) =>
                pow(f1 - f2, 2)
              }).sum
            )
          )
        )

      val newCentroids = data map(x => x.Centroid)
      val changed = ((oldCentroids zip newCentroids) map {case (x, y) => if (x.equals(y)) { 0 } else { 1 }}).sum
      sender ! changed

    case GetNewCentroids =>
      // println("Doing new centroids " + hash)

      def emptyFeatures(numbFeatures: Int) : Array[Double] = {
        val features = for (i <- 0 to numbFeatures - 1)
          yield 0.0
        features.toArray
      }

      def centroidsMap(dataSet: Array[CentroidSuggestion], item: DataPoint): Array[CentroidSuggestion] = {
        dataSet map(x =>
            if (x.Centroid != item.Centroid) {
              x
            } else {
              updateCentroidSuggestion(x.Centroid, (x.Features zip item.Features) map {case (f1, f2) => f1 + f2}, x.Count + 1)
            }
          )
      }

      def generateNewCentroids(newCentroids: Array[CentroidSuggestion], dataPoints: Array[DataPoint]) : Array[CentroidSuggestion] = {
        if (dataPoints.length > 0 && dataPoints.tail.length > 0) {
          generateNewCentroids(centroidsMap(newCentroids, dataPoints.head), dataPoints.tail)
        } else {
          centroidsMap(newCentroids, dataPoints.head)
        }
      }

      val centroidSuggestions = centroids map(x =>
        new CentroidSuggestion(x.Name, emptyFeatures(x.Features.length), 0)
        )

      sender ! generateNewCentroids(centroidSuggestions, data)

    case GetDataPoints =>
      // println("Sending data points " + hash)
      sender ! data

    case UpdateCentroids(newCentroids) =>
      // println("Updating centroids " + hash)
      centroids = newCentroids
  }
}

object dptKmeans {
  def emptyFeatures(numbFeatures: Int) : Array[Double] = {
    val features = for (i <- 0 to numbFeatures - 1)
      yield 0.0
    features.toArray
  }

  def sumFeatures(centroidSuggestions: Array[CentroidSuggestion], sum: Array[Double], count: Int) : Array[Double] = {
    if (centroidSuggestions.nonEmpty) {
      sumFeatures(centroidSuggestions.tail, (centroidSuggestions.head.Features zip sum) map {case (f1, f2) => f1+f2}, count + centroidSuggestions.head.Count)
    } else {
      sum map (x => if (count == 0) { 0.0 } else { x / count.toDouble })
    }
  }

  def readInput(filename: String) = {
    for (line <- Source.fromFile(filename).getLines().map(_.split(",")))
    yield new DataPoint(line.reverse.tail.reverse.map(number => number.toDouble), 0, line.reverse.head.toInt)
  }

  def dataPartition(data: Array[DataPoint], output: Array[Array[DataPoint]], defaultSize: Int, remainingRest: Int): Array[Array[DataPoint]] = {
    if (data.nonEmpty) {
      val thisPartitionSize = if (remainingRest > 0) { defaultSize + 1 } else { defaultSize }
      val nextRemainingRest = if (remainingRest > 0) { remainingRest - 1 } else { remainingRest }
      val thisPartition = data.slice(0, thisPartitionSize)
      val remainingData = data.drop(thisPartitionSize)

      dataPartition(remainingData, output :+ thisPartition, defaultSize, nextRemainingRest)
    } else {
      output
    }
  }

  def kmeans(iteration: Int, actors: Array[ActorRef]) : Boolean = {
    implicit val timeout = new Timeout(600.seconds)

    if (iteration == 0) {
      // println("No more iterations")
      return true
    }

    // Ask for clustering and changes in points
    val sumChanged = ((actors map (actor => actor ask KmeansActor.Cluster)) map (future => Await.result(future, timeout.duration).asInstanceOf[Int])).sum

    // Ask for data points and calculate statistics
    val changedDataPoints = (actors map (actor => actor ask KmeansActor.GetDataPoints) map (future => Await.result(future, timeout.duration).asInstanceOf[Array[DataPoint]])).flatten
    val percentage = (changedDataPoints map (x => if (x.Centroid == x.Label) { 1 } else { 0 })).sum.toDouble / changedDataPoints.length.toDouble * 100.0
    println("Percentage (iteration: " + iteration + ", changed: " + sumChanged + "): " + percentage)

    // Get new centroids and update them
    // println("Calculating new centroids")
    val centroidSuggestions = (((actors map (actor => actor ask KmeansActor.GetNewCentroids)) map (future => Await.result(future, timeout.duration).asInstanceOf[Array[CentroidSuggestion]])).flatten
      groupBy(x => x.Centroid)
      map {case (index, x) =>
      new Centroid(index, sumFeatures(x.toArray, emptyFeatures(x.head.Features.length), 0))
    })
    // println("Done calculating new centroids")

    if (sumChanged == 0) {
      println("Percentage Score: " + percentage + "%")
      // println("No more changes")
      return true
    }

    actors foreach (actor => actor ! KmeansActor.UpdateCentroids(centroidSuggestions.toArray))

    kmeans(iteration - 1, actors)
  }

  def main(args: Array[String]) {
    // Load data
    val numbPartitions = if (args.length > 0) { args(0).toInt } else { 1 }
    val numbCentroids = if (args.length > 1) { args(1).toInt } else { 3 }
    val dataPointsPath = if (args.length > 2) { args(2) } else { "C:\\Users\\ThomasStig\\IdeaProjects\\dptKmeans\\src\\1000-100-3.csv" }
    val numbIterations = 300
    println("Maximum Iterations: " + numbIterations)
    println("Dataset: " + dataPointsPath)

    val dataPoints = readInput(dataPointsPath).toArray

    // val centroids = dataPoints.slice(0, numbCentroids).map(x => new Centroid(x.hashCode(), x.Features))
    val centroids = for (i <- 0 to numbCentroids - 1)
        yield new Centroid(i, dataPoints.apply(i).Features)

    // Partition data
    val dataPartitionSize = math.floor(dataPoints.length / numbPartitions.toDouble).toInt
    val dataPartitionRest = dataPoints.length - (numbPartitions * dataPartitionSize)
    val dataPartitions = dataPartition(dataPoints, Array(), dataPartitionSize, dataPartitionRest)

    // Setup Actor system
    val system = ActorSystem("actorSystem")
    val actors = dataPartitions map(segment => system.actorOf(Props(new KmeansActor(segment.toArray, centroids.toArray))))

    val time1 = System.nanoTime()

    kmeans(numbIterations, actors)

    val time2 = System.nanoTime()
    val timeDiff = time2 - time1
    println("Execution Time: " + (timeDiff / 1000000000.0))
    System.exit(1)
  }
}
