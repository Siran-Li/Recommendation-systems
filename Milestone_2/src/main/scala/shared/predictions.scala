package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

package object predictions
{
  // ------------------------ For template
  case class Rating(user: Int, item: Int, rating: Double)

  /** Computes the time required to run a function given
   *
   *  @param f function to be measured following the signature given
   *  @return a tuple containing the result of the method and the time to run
   * in Ms
   */
  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

    /** Converts string value to Int
   * 
   */
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0

  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else { 
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble) 
    }
  }


  def load(path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = Source.fromFile(path)
    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies) 
    for (line <- file.getLines) {
      val cols = line.split(sep).map(_.trim)
      toInt(cols(0)) match {
        case Some(_) => builder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
        case None => None
      }
    }
    file.close
    builder.result()
  }

  def loadSpark(sc : org.apache.spark.SparkContext,  path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = sc.textFile(path)
    val ratings = file
      .map(l => {
        val cols = l.split(sep).map(_.trim)
        toInt(cols(0)) match {
          case Some(_) => Some(((cols(0).toInt-1, cols(1).toInt-1), cols(2).toDouble))
          case None => None
        }
      })
      .filter({ case Some(_) => true
                 case None => false })
      .map({ case Some(x) => x
             case None => ((-1, -1), -1) }).collect()

    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies)
    for ((k,v) <- ratings) {
      v match {
        case d: Double => {
          val u = k._1
          val i = k._2
          builder.add(u, i, d)
        }
      }
    }
    return builder.result
  }

  def partitionUsers (nbUsers : Int, nbPartitions : Int, replication : Int) : Seq[Set[Int]] = {
    val r = new scala.util.Random(1337)
    val bins : Map[Int, collection.mutable.ListBuffer[Int]] = (0 to (nbPartitions-1))
       .map(p => (p -> collection.mutable.ListBuffer[Int]())).toMap
    (0 to (nbUsers-1)).foreach(u => {
      val assignedBins = r.shuffle(0 to (nbPartitions-1)).take(replication)
      for (b <- assignedBins) {
        bins(b) += u
      }
    })
    bins.values.toSeq.map(_.toSet)
  }



  // --------------------------- CODE FROM BEFORE --------------------------
  // ----------------------- General utils ----------------------------------

  /** Scales the user by the average user rating
   * 
   * @param x the rating given by user
   * @param average the average of the ratings given by users
   * @return the scaled rating
   * 
   */
  def scale_value(x: Double, average: Double): Double = {
    if (x > average) return 5 - average
    if (x < average) return average - 1

    return 1.0
  }


  /** Computes the normalized deviation from the rating given by user and the
  * average of the ratings given by the user
  * 
  * @param rating the rating given by user
  * @param avgRatingPerUser map containing the average rating for each user ID
  * @return normalized deviation
  * 
  */
  def normalize(rating: Double, avgRatingPerUser: Double): Double = {
    return (rating - avgRatingPerUser) / scale_value(rating, avgRatingPerUser)
  }


  /** Computes the normalized deviation from the rating given by user and the
  * average of the ratings given by the user fo sparse matrices
  * 
  * @param rating the rating given by user
  * @param avgRatingPerUser map containing the average rating for each user ID
  * @return normalized deviation
  * 
  */
  def computeNormalizedDeviation(train: CSCMatrix[Double], avgRatingPerUser: DenseVector[Double]): CSCMatrix[Double] = {
    val normalizedDev = new CSCMatrix.Builder[Double](rows = train.rows, cols = train.cols)
    
    for ((k, v) <- train.activeIterator) {
      val user = k._1
      val item = k._2

      normalizedDev.add(user, item, normalize(v, avgRatingPerUser(user)))
    }
    
    return normalizedDev.result()
  }

  /** Computes the MAE for a given predictor
   * 
   * @param test the dataset for which we want to test the predictor's error
   * @param predictor the predictor used to estimate the ratings
   * @return the mean average error
   * 
   */
  def MAE(test: CSCMatrix[Double], predictor: (Int, Int) => Double): Double = {
    var count = 0
    for ((k, v) <- test.activeIterator) {
      count += 1
      test(k._1, k._2) = abs(v - predictor(k._1 + 1, k._2 + 1))
    }
    return sum(test) / count
  }


  /** Computes the MAE for a given predictor parallel
   * 
   * @param test the dataset for which we want to test the predictor's error
   * @param predictor the predictor used to estimate the ratings
   * @return the mean average error
   * 
   */
  def MAEParallel(sc: SparkContext, test: CSCMatrix[Double], predictor: (Int, Int) => Double): Double = {
    val seq = test.activeIterator.toSeq

    def compute_prediction(value: ((Int, Int), Double)): Double = {
      return abs(value._2 - predictor(value._1._1 + 1, value._1._2 + 1))
    }
    val sum = sc.parallelize(seq).map(compute_prediction).reduce{(x,y) => x + y}
      
    return sum / seq.length
  }


  /** Preprocess the rating by dividing the square root of the summation
   * 
   * @param input array of all ratings given
   * @return array of all the ratings given where rating has been normalized
   * by the square root of the summation of the corresponding set of squared 
   * rating value
   */
  def preprocessRatings(input: CSCMatrix[Double]): CSCMatrix[Double] = {
    val preprocessRatingsBuilder = new CSCMatrix.Builder[Double](rows = input.rows, cols = input.cols)

    val squared_inputs = input.mapActiveValues(x => x*x)
    val reducer = DenseVector.ones[Double](input.cols)
    val denominator = sqrt(squared_inputs * reducer).mapActiveValues(x => if (x == 0.0) 1.0 else x)

    for ((k, v) <- input.activeIterator) {
      val user = k._1
      val col = k._2
      preprocessRatingsBuilder.add(user, col, v / denominator(user))
    }

    return preprocessRatingsBuilder.result()
  }


  /** Compute the user-specific weighted-sum deviation parallel.
   * 
   * @param train array of all ratings given
   * @param normalizedDev the result array from function preprocessing_rating
   * @param similarities the similarity matrix 
   * @return a matrix with size NxB, where N is the total number of users,
   * and B is the total number of items
   */
  def computeUserSpecDevPerItemOnly(
      user: Int, item: Int,
      normalizedDev: CSCMatrix[Double],
      similarities: CSCMatrix[Double],
      train_exists: CSCMatrix[Double]
  ): Double = {

    val dev_i = normalizedDev(0 to similarities.rows - 1, item).toDenseVector
    val similarities_u = similarities(user, 0 to similarities.rows - 1).t.toDenseVector

    var nominator_u_i = 0.0
    var denominator_u_i = 0.0

    for ((user_v, dev) <- dev_i.activeIterator) {
        nominator_u_i = nominator_u_i + dev * similarities_u(user_v)
        denominator_u_i = denominator_u_i + (if (train_exists(user_v, item) != 0.0) math.abs(similarities_u(user_v)) else 0.0)
    }

    return if (denominator_u_i != 0.0) nominator_u_i / denominator_u_i else 0.0
  }


  /** Compute the predictions for users
   *
   * @param user the ID of the user
   * @param item the ID of the item
   * @param globalAverage the global average computed from all existing ratings
   * @param avgRatingPerUser map containing the average rating for each user ID
   * @param userSpecDevPerItem array containing the user-specific weighted-sum 
  *  deviation for each user ID and item ID
   * @return prediction made by the cosine similarities for the given user ID and item ID
   *
   */
  def computePersonalizedPrediction(
      user: Int,
      item: Int,
      globalAvg: Double,
      avgRatingPerUsers: DenseVector[Double],
      normalizedDev: CSCMatrix[Double],
      similarities: CSCMatrix[Double],
      train_exists: CSCMatrix[Double]
  ): Double = {
    var r_u: Double = globalAvg
    var r_i: Double =  0.0
    
    if (avgRatingPerUsers(user - 1) != 0.0) {
      r_u = avgRatingPerUsers(user - 1)
      
      if (item < normalizedDev.cols) {
        r_i = computeUserSpecDevPerItemOnly(user - 1, item - 1, normalizedDev, similarities, train_exists)
      }
    } else {
      return globalAvg 
    }
    return r_u + r_i * scale_value((r_u + r_i), r_u)
  }


  // -------------------- N -----------------------------

  /** Computes the similiarities matrix from the prior ratings only for K
   * closest neighbours by similarity, otherwise 0.
   * 
   * @param train array containing all ratings given
   * @param normalizedDev normalized ratings for each rating given
   * @param k number of neighbours to be considered
   * @return matrix containing the adjusted cosine similarity between any two
   * users that gave ratings before
   */ 
  def computeKnnSimilarities(
      normalizedDev: CSCMatrix[Double],
      k: Int
  ): CSCMatrix[Double] = {

    val prerating_matrix = preprocessRatings(normalizedDev)
    val builder = new CSCMatrix.Builder[Double](rows=prerating_matrix.rows, cols=prerating_matrix.rows)

    (0 to prerating_matrix.rows-1).foreach{user =>
      var similarities_u = prerating_matrix * prerating_matrix(user, 0 to prerating_matrix.cols - 1).t
      similarities_u(user) = 0

      val topks = argtopk(similarities_u.toDenseVector, k).toArray.map({ case v: Int => (v, similarities_u(v))})
      
      for ((k,v) <- topks) {
        builder.add(user, k, v)
      }
    }

    return builder.result()
  }


  /** Computes the similiarities matrix from the prior ratings only for K
   * closest neighbours by similarity, otherwise 0, using parallelization.
   * 
   * @param train array containing all ratings given
   * @param normalizedDev normalized ratings for each rating given
   * @param k number of neighbours to be considered
   * @return matrix containing the adjusted cosine similarity between any two
   * users that gave ratings before
   */ 
  def computeKnnSimilaritiesParallel(
      sc: SparkContext,
      normalizedDev: CSCMatrix[Double],
      k: Int
  ): CSCMatrix[Double] = {
    val prerating_matrix = preprocessRatings(normalizedDev)
    val prerating_matrix_broadcasted = sc.broadcast(prerating_matrix)

    def topk(user: Int): (Int, Array[(Int, Double)]) = {
      // Extracts the topk neighbours and their similarity to the current user
      val preratings = prerating_matrix_broadcasted.value

      var similarities_u = preratings * preratings(user, 0 to preratings.cols - 1).t
      similarities_u(user) = 0.0
      
      // It is possible to encounter negative similarities when computing the
      // topk values and we decided to filter out and keep only positive similarities
      // in top k neighbours.

      // Moreover, for the case where the number of users in a partition is smaller
      // than the K chosen for the KNN algorithm, the argtopk would retrieve wrongly
      // a high amount of zero values representing similarities with users
      // from a different partition. Thus we also restrict it to non-zero similarities.
      return (user, 
      argtopk(similarities_u.toDenseVector, k).toArray.filter(_ > 0.0).map(
      { case v: Int => (v, similarities_u(v))}))
    }

    val topks = sc.parallelize(0 to prerating_matrix.rows - 1).map(topk).collect()

    // Constructs the matrix containg all similarity values
    def knnBuilder(topks: Array[(Int, Array[(Int, Double)])]): CSCMatrix[Double] = {
      val builder = new CSCMatrix.Builder[Double](rows=prerating_matrix.rows, cols=prerating_matrix.rows)
      topks.map{ case (userU, user_sim) => {
        user_sim.map{ case(userV, simi) => {
          builder.add(userU, userV, simi)
        }
        }
        }
      }
      return builder.result()
    }

    return knnBuilder(topks)
  }


  /** Creates a predictor for the KNN-based prediction method 
   * 
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def knnPredictor(train: CSCMatrix[Double], k: Int): ((Int, Int) => Double) = {
    val train_exists = train.mapActiveValues(x => if (x != 0.0) 1.0 else 0.0)
    val globalAvg = sum(train) /:/ sum(train_exists)

    val avgRatingPerUsers = computeAvgRatingPerUser(train, train_exists)

    val normalizedDev = computeNormalizedDeviation(train, avgRatingPerUsers)

    val knnSimilarities = computeKnnSimilarities(normalizedDev, k)

    (u: Int, i: Int) => {
      computePersonalizedPrediction(
        u,
        i,
        globalAvg,
        avgRatingPerUsers,
        normalizedDev,
        knnSimilarities,
        train_exists
      )
    }
  }

  /** Computes the average rating per user using matrix calculation.
   * 
   * @param train the data used to train the predictor
   * @param train_exists Boolean matrix containing 1 where a value exists in train
   * @return a vector with the size of number of users and the average rating for each
   * 
   */
  def computeAvgRatingPerUser(train: CSCMatrix[Double], train_exists: CSCMatrix[Double]): DenseVector[Double] = {
      val reducer = DenseVector.ones[Double](train.cols)
      return (train * reducer) /:/ (train_exists * reducer)
  }

  /** Creates a predictor for the KNN-based prediction using Spark parallelization.
   * 
   * @param context the Spark Context required for parallelization
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def knnPredictorParallel(context: SparkContext, train: CSCMatrix[Double], k: Int): ((Int, Int) => Double) = {
    val train_exists = train.mapActiveValues(x => if (x != 0.0) 1.0 else 0.0)
    val globalAvg = sum(train) /:/ sum(train_exists)

    val avgRatingPerUsers = computeAvgRatingPerUser(train, train_exists)

    val normalizedDev = computeNormalizedDeviation(train, avgRatingPerUsers)

    val knnSimilarities = computeKnnSimilaritiesParallel(context, normalizedDev, k)

    (u: Int, i: Int) => {
      computePersonalizedPrediction(
        u,
        i,
        globalAvg,
        avgRatingPerUsers,
        normalizedDev,
        knnSimilarities,
        train_exists
      )
    }
  }

  /** Creates a predictor for the KNN-based prediction using an approximation based on users partitions
   * 
   * @param context the Spark Context required for parallelization
   * @param train the data used to train the predictor
   * @return predictor that expects two integer values, the user ID and item ID
   * and returns the prediction
   *
   */
  def distributedPredictor(context: SparkContext, train: CSCMatrix[Double], k: Int, partitionedUsers: Seq[Set[Int]]): ((Int, Int) => Double) = {
    val train_exists = train.mapActiveValues(x => if (x != 0.0) 1.0 else 0.0)
    val globalAvg = sum(train) /:/ sum(train_exists)

    val avgRatingPerUsers = computeAvgRatingPerUser(train, train_exists)

    val normalizedDev = computeNormalizedDeviation(train, avgRatingPerUsers)

    val knnSimilarities = distributedSimilarities(context, normalizedDev, k, partitionedUsers)

    (u: Int, i: Int) => {
      computePersonalizedPrediction(
        u,
        i,
        globalAvg,
        avgRatingPerUsers,
        normalizedDev,
        knnSimilarities,
        train_exists
      )
    }
  }

  /** Computes the similiarities matrix from the prior ratings only for K
   * closest neighbours by similarity, otherwise 0, using parallelization
   * and approximation based on users partitions.
   * 
   * @param train array containing all ratings given
   * @param normalizedDev normalized ratings for each rating given
   * @param k number of neighbours to be considered
   * @param partitionedUsers the split of users within partitions
   * @return matrix containing the adjusted cosine similarity between any two
   * users that gave ratings before
   */ 
  def distributedSimilarities (sc: SparkContext, normalizedDev: CSCMatrix[Double], k: Int, partitionedUsers: Seq[Set[Int]]): CSCMatrix[Double] ={

    val prerating_matrix = preprocessRatings(normalizedDev)
    val prerating_matrix_broadcasted = sc.broadcast(prerating_matrix)

    def partialUserTopK(partition_idx: Int): Array[(Int, Array[(Int, Double)])] = {
      // Computes the topk neighbours for each user within current partition

      val index_array = partitionedUsers(partition_idx).toArray
      val preratings = prerating_matrix_broadcasted.value  
      val build_preratings_part = new CSCMatrix.Builder[Double](rows=preratings.rows, cols=preratings.cols)

      // Construct matrix that only contains the users within current partition
      for (v_i <- index_array){
        var preratings_i = preratings(v_i, 0 to preratings.cols - 1)
        for (i <- (0 until preratings.cols)){
            build_preratings_part.add(v_i, i, preratings_i(i))
        }
        // Set self-similarity to 0
        build_preratings_part.add(v_i, v_i, 0.0)
      }

      val preratings_part = build_preratings_part.result()

      // Method to retrieve the topk neighbours for one user within current partition
      def part_topk(user: Int): (Int, Array[(Int, Double)]) = {
        var similarities_u = preratings_part * preratings_part(user, 0 to preratings.cols - 1).t
        similarities_u(user) = 0.0

        // It is possible to encounter negative similarities when computing the
        // topk values and we decided to filter out and keep only positive similarities
        // in top k neighbours.
        //
        // Moreover, for the case where the number of users in a partition is smaller
        // than the K chosen for the KNN algorithm, the argtopk would retrieve wrongly
        // a high amount of zero values representing similarities with users
        // from a different partition. Thus we also restrict it to non-zero similarities.
        return (user, argtopk(similarities_u.toDenseVector, k).toArray.filter(
        _ > 0.0).map({ case v: Int => (v, similarities_u(v))}))
      }

      val part_topks = (index_array).map(part_topk)

      return part_topks
    }

    // Combines the results efficently from the RDD collection of topk similarities
    // list within each partition, and keeps for each any two users all 
    // similarities that have been retrieved among the topk from different
    // partitions.
    // 
    // This way we can keep only the maximum similarity between two users
    // when they end up together in multiple partitions.
    val result = sc.parallelize(0 to partitionedUsers.length - 1).map(
    partialUserTopK).reduce(_ ++ _).groupBy(x => x._1).mapValues(
    seq => seq.map(x => x._2)).mapValues(seq => seq.reduce(_ ++ _))
    
    val builder_similarities = new CSCMatrix.Builder[Double](rows=prerating_matrix.rows, cols=prerating_matrix.rows)
    (result).foreach( (user_array: (Int, Array[(Int, Double)])) => {
        // Retrieve the maximum similarity for two users among all partitions
        val values = user_array._2.groupBy(x => x._1).mapValues(seq => seq.reduce{(x,y) => (x._1, max(x._2, y._2))}).map(x => x._2)
        val u1 = user_array._1 
        values.foreach( (top_value: (Int, Double)) => builder_similarities.add(u1, top_value._1, top_value._2) )
      })
    val tot_knnSimilarities = builder_similarities.result()

    // Compute the topk similarity similarily to the exact KNN method 
    // on the matrix computed from the separate partitions
    def topk_sim(user: Int): (Int, Array[(Int, Double)]) = {
      var sim_u = tot_knnSimilarities(user, 0 to prerating_matrix.rows - 1).t
      sim_u(user) = 0.0

      return (user, argtopk(sim_u, k).toArray.map({ case v: Int => (v, sim_u(v))}))
    } 

    val topks = sc.parallelize(0 to prerating_matrix.rows - 1).map(topk_sim).collect()

    def knnBuilder(topks: Array[(Int, Array[(Int, Double)])]): CSCMatrix[Double] = {
      val builder = new CSCMatrix.Builder[Double](rows=prerating_matrix.rows, cols=prerating_matrix.rows)
      for (user_sim <- topks) {
        val user = user_sim._1
        for (values <- user_sim._2) {
          builder.add(user, values._1, values._2)
        }
      }
      return builder.result()
    }

    val knnSimilarities = knnBuilder(topks)   

    return knnSimilarities

  }
}
