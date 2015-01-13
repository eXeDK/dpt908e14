;; An STM Based Parallel K-Means Implementation Written in Clojure
;; ============================================================================
;; [DONE] Linted Using
;; https://github.com/jonase/eastwood
;; https://github.com/jonase/kibit
;; https://github.com/dakrone/lein-bikeshed
;;
;; Sources of inspiration
;; ============================================================================
;; http://en.wikibooks.org/wiki/Data_Mining_Algorithms_In_R/Clustering/K-Means
;;
;; http://clojure.org/cheatsheet
;; http://clojure.org/concurrent_programming


;; Imports
;;;;;;;;;;
(require '[clojure.java.io :as io])
(require '[clojure.string :as str])


;; Data Structures
;;;;;;;;;;;;;;;;;;
(defrecord Point [features cluster label])
(defrecord Centroid [features cluster])

; NOTE: java.util.concurrent.LinkedBlockingQueue is used with STM despite knowledge
;   about the internal synchronisation mechanism's of the class as Clojure's, to
;   my knowledge, only blocking collection "seque" uses an instance of the class
;   internally, the extra synchronisation might add however add execution overhead
; Link: https://github.com/clojure/clojure/blob/master/src/clj/clojure/core.clj#L5106
(def send-clustered-points-queue (ref (java.util.concurrent.LinkedBlockingQueue.)))
(def send-centroid-proposal-queue (ref (java.util.concurrent.LinkedBlockingQueue.)))
(def recieve-updated-centroid-queue (ref (java.util.concurrent.LinkedBlockingQueue.)))

; A barrier is needed to synchronize the threads between each run of K-Means, as threads
; might otherwise be starved by the runtime due to unfair scheduling behaviour
(def barrier (ref nil))

(defn stm-put [queue-ref elem]
  (dosync (.put (deref queue-ref) elem)))

(defn stm-take [queue-ref]
  (dosync (.take (deref queue-ref))))

(defn barrier-setup [number-of-threads]
  (dosync (ref-set barrier  (java.util.concurrent.CyclicBarrier. number-of-threads))))

(defn barrier-await []
  (dosync (.await (deref barrier))))


;; Helper Functions
;;;;;;;;;;;;;;;;;;;
(defn partition-data [points partition-size leftover-count acc]
  (cond
    (empty? points) (vec acc)
    (zero? leftover-count) (let [split (split-at partition-size points)]
                             (partition-data (second split) partition-size leftover-count (conj acc (first split)))
                             )
    :else (let [split (split-at (inc partition-size) points)]
            (partition-data (second split) partition-size (dec leftover-count) (conj acc (first split))))))

(defn add-centroid-proposal [cp1 cp2]
  [(+ (first cp1) (first cp2)) (map + (second cp1) (second cp2))])

(defn send-centroids [centroids all-finished number-of-threads]
  (doseq [ct (vec (repeat number-of-threads [all-finished, centroids]))]
    (stm-put recieve-updated-centroid-queue ct)))

(defn contains-alpha? [line]
  ; A match for two letters are required to prevent matching 2e-2 float notation
  (not= (re-find #"[a-zA-Z]{2}" line) nil))

(defn print-n-elems [n elements]
  (if (== -1 n)
    (doseq [elem elements] (prn elem))
    (doseq [elem (take n elements)] (prn elem))))

(defn repeatv [n x]
  (vec (repeat n x)))

(defn concatv [xs ys]
  (vec (concat xs ys)))

(defn time-function [function & args]
  (let [starttime (System/nanoTime)]
    [(apply function args)
     (/ (- (System/nanoTime) starttime) 1e9)]))

(defn compute-correct-classefied-percentage [points]
  (if (== (:label (first points)) -1) 0.0
    (let [correct-clustered-count (reduce (fn [acc elem] (if (== (:cluster elem) (:label elem)) (inc acc) acc)) 0 points)]
      (* (double (/ correct-clustered-count (count points))) 100))))


;; I/O Functions
;;;;;;;;;;;;;;;;
(defn ->point-with-label [line]
  (let [split-string (str/split line #",")
        features (mapv (fn [elem] (Double/parseDouble elem)) (butlast split-string))
        label (Integer/parseInt (last split-string))]
    (->Point features -1 label)))

(defn ->point-without-label [line]
  (->Point (mapv (fn [elem] (Double/parseDouble elem)) (str/split line #",")) -1 -1)
  )

(defn read-dataset [file-path delimiter contains-labels]
  (let [->point (if contains-labels ->point-with-label ->point-without-label)]
    (with-open [file-reader (io/reader file-path)]
      (vec (reverse (doall (reduce
                             (fn [acc line] (if-not (contains-alpha? line) (cons (->point line) acc)))
                             () (line-seq file-reader))))))))


;; Distance Functions
;;;;;;;;;;;;;;;;;;;;;
(defn compute-eucledian-distance [v1 v2]
  (reduce + (mapv (fn [a b] (Math/pow (- a b) 2)) v1 v2)))

;; K-Means Phase 2: Recompute the position of the centroid for each cluster
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn add-point-to-centroid-acc [centroid-acc point]
  (let [cluster (:cluster point)
        old-value (get centroid-acc cluster)
        new-value [(inc (first old-value)) (mapv + (:features point) (second old-value))]]
    (assoc centroid-acc cluster new-value)))

(defn update-centroids [points n_clusters]
  (let [n_features (count (:features (first points)))
        centroid-acc (repeatv n_clusters [0 (repeatv n_features 0.0)])]
    (reduce add-point-to-centroid-acc centroid-acc points)))

;; K-Means Phase 1: Reassign all points to the best possible centroid
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn compute-best-cluster [point centroids best-distance best-cluster]
  (if (empty? centroids) best-cluster
    (let [new-distance (compute-eucledian-distance (:features point) (:features (first centroids)))]
      (if (< new-distance best-distance)
        (compute-best-cluster point (rest centroids) new-distance (:cluster (first centroids)))
        (compute-best-cluster point (rest centroids) best-distance best-cluster)))))

(defn update-points [points centroids]
  (mapv (fn [point]
          (->Point (:features point) (compute-best-cluster point centroids (Double/MAX_VALUE) 0) (:label point)))
        points))


;; K-Means Parallel Runner
;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn retrieve-clustered-points [number-of-threads acc]
  (cond
    (zero? number-of-threads) acc
    :else (retrieve-clustered-points (dec number-of-threads) (concatv (stm-take send-clustered-points-queue) acc))))

(defn points-changed? [points updated-points is-changed?]
  (cond
    is-changed? true
    (empty? points) false
    :else (recur (rest points) (rest updated-points)
                 (not= (:cluster (first points)) (:cluster (first updated-points))))))

(defn divide-centroid-features [summed-centroid cluster]
  (let [point-count (first summed-centroid)
        feature-summed (second summed-centroid)]
    (if (zero? point-count)
      (->Centroid feature-summed cluster)
      (->Centroid (mapv (fn [x] (/ x point-count)) feature-summed) cluster))))

(defn compute-updated-centroids-from-proposals [number-of-threads]
  (defn compute-centroids [threads-running all-finished acc]
    (cond
      ; All threads have returned their centroids so we compute the final new centroids
      (zero? threads-running) [all-finished (mapv divide-centroid-features acc (range))]
      ; Some threads still need to return their centroids so we wait for some to appear in the blocking queue
      :else (let [centroid-proposal (stm-take send-centroid-proposal-queue)
                  points-changed (first centroid-proposal)
                  centroids (second centroid-proposal)]
              ; Prevents "map" from returning the empty list when we receive the first set of centroids
              (if (empty? acc)
                (compute-centroids (dec threads-running) (and (not points-changed) all-finished) centroids)
                (compute-centroids (dec threads-running) (and (not points-changed) all-finished) (map add-centroid-proposal centroids acc))))))
  (let [all-finished-centroids (compute-centroids number-of-threads true '())]
    (send-centroids (second all-finished-centroids) (first all-finished-centroids) number-of-threads)
    ; If all threads have terminated can this function just exit, if not then we wait for a new set of proposals
    (when-not (first all-finished-centroids)
      (compute-updated-centroids-from-proposals number-of-threads))))

(defn run-k-means [points centroids iteration n_points n_clusters]
  (cond
    (zero? iteration) (do
                        (stm-put send-centroid-proposal-queue [false '()])
                        (stm-put send-clustered-points-queue points))
    :else (let [updated-points (update-points points centroids)
                proposed-centroids (update-centroids updated-points n_clusters)]
            ; The new points and centroids are computed, so centroids are send to the aggregation function
            (stm-put send-centroid-proposal-queue [(points-changed? points updated-points false) proposed-centroids])
            ; Prevents a single thread to run multiple iterations
            (barrier-await)
            ; The boolean indicating that all threads have finished and the list of new complete new centroids
            (let [all-finished-centroids (stm-take recieve-updated-centroid-queue)]
              ; If all threads have finished can we freely send the points, else we have to run again
              (if (first all-finished-centroids)
                (stm-put send-clustered-points-queue points)
                (run-k-means updated-points (second all-finished-centroids) (dec iteration) n_points n_clusters))))))

;; K-Means Interface
;;;;;;;;;;;;;;;;;;;;
(defn k-means [points centroids max-iterations number-of-threads]
  (let [n_points (count points)
        n_clusters (count centroids)
        partition-size (int (Math/floor (/ n_points number-of-threads)))
        partitioned-points (partition-data points partition-size (mod n_points number-of-threads) [])]
    ; Threads are started using futures, but all communication are done through
    ; queue, this is to match the Haskell version more and utilise STM for all
    (doseq [point-partition partitioned-points]
      (future (run-k-means point-partition centroids max-iterations (count point-partition) n_clusters)))
    (compute-updated-centroids-from-proposals number-of-threads)
    (retrieve-clustered-points number-of-threads [])))

;; Main
;;;;;;;
(def iris-test-centroids
  (mapv ->Centroid
        [[5.006 3.418 1.464 0.244] [5.9016129 2.7483871 4.39354839 1.43387097] [6.85, 3.07368421 5.74210526, 2.07105263]]
        (range)))

(defn take-n-points-as-centroids [n dataset]
  (mapv (fn [point cluster] (->Centroid (:features point) cluster)) (take n dataset) (range)))

(defn main [number-of-threads number-of-centroids dataset-filepath]
  (println "Parsing Dataset:  " (str (java.sql.Timestamp. (.getTime (java.util.Date.)))))
  (let [max-iterations 300
        dataset (read-dataset dataset-filepath "," true)
        centroids (take-n-points-as-centroids number-of-centroids dataset)]
    (barrier-setup number-of-threads)
    (println "Starting K-Means: " (str (java.sql.Timestamp. (.getTime (java.util.Date.)))))
    (let [clusters-and-time (time-function k-means dataset centroids max-iterations number-of-threads)
          clusters (first clusters-and-time)
          k-means-execution-time (second clusters-and-time)]
      (println "Finnished K-Means:" (str (java.sql.Timestamp. (.getTime (java.util.Date.)))))
      (println "\nDataset:" dataset-filepath)
      (println "Maximum Iterations:" max-iterations)
      (println "Execution Time:" k-means-execution-time)
      (println "Percentage Score:" (str (compute-correct-classefied-percentage clusters) "%"))
      (println "\nCentroids:") (print-n-elems -1 centroids))))

; Prints usage and terminates if the user have not passed a list of threads
(when-not (== 3 (count *command-line-args*))
  (println "usage: KMeansClojure number-of-threads number-of-centroids dataset-filepath")
  (System/exit -1))

; Extract the number of threads from the command line arguments
(main (Integer. (first *command-line-args*)) (Integer. (second *command-line-args*)) (nth *command-line-args* 2))

; Forces Clojure to terminate dissipate the futures not being used
(shutdown-agents)
