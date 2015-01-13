; Global Variables and Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def santa-thread-ref (ref '()))
(def elf-queue-ref (ref '()))
(def reindeer-queue-ref (ref '()))

(def worker-lock (Object.))
(def santa-sleep-lock (Object.))

(def enough-reindeers-to-wake-santa 9)
(def enough-elfs-to-wake-santa 3)

;; Helper Functions
;;;;;;;;;;;;;;;;;;;
(defn elem? [elem collection]
  (cond
    (empty? collection) false
    (== elem (first collection)) true
    :else (elem? elem (rest collection))))

(defn goto-santa [santas-door id]
  (let [queue-at-door (deref santas-door)]
    (if (elem? id queue-at-door)
      (throw (IllegalArgumentException.))
      (alter santas-door conj id))))

(defn sleep-random-interval [n]
  (Thread/sleep (* 1000 (rand-int n))))

(defn block-worker []
  (locking worker-lock
    (.wait worker-lock)))

(defn notify-all-workers []
  (locking worker-lock
    (.notifyAll worker-lock)))

;; Worker
;;;;;;;;;;
(declare wake-santa)
(defn do-work [wid maxSleep queue-ref maxQueue]
  (try
    (locking santa-sleep-lock
      (dosync
        (let [current-queue-length (count (deref queue-ref))]
          (if (< current-queue-length maxQueue)
            (goto-santa queue-ref wid)
            (throw (IllegalArgumentException.)))
          (when (== (inc current-queue-length) maxQueue)
            (wake-santa)))))
    (catch IllegalArgumentException iae
      (block-worker)
      (do-work wid maxSleep queue-ref maxQueue))))

(defn worker [id maxSleep queue-ref maxQueue]
  (sleep-random-interval maxSleep)
  ; Seperating the Worker into two makes it simple to retry without sleeping
  (do-work id maxSleep queue-ref maxQueue)
  (recur id maxSleep queue-ref maxQueue))

;; Santa
;;;;;;;;
(defn sleep-santa []
  (let [santa-thread (deref santa-thread-ref)]
    (locking santa-thread
      (notify-all-workers)
      (.wait santa-thread))
    (locking santa-sleep-lock)))

(defn wake-santa []
  (let [santa-thread (deref santa-thread-ref)]
    (locking santa-thread
      (if (= (.getState santa-thread) java.lang.Thread$State/WAITING)
        (.notify santa-thread)
        (throw (IllegalArgumentException.))))))

(defn work-santa [workers queue phrase]
  (println "Santa: ho ho" phrase workers)
  (ref-set queue '())
  (sleep-random-interval 5))


(defn santa []
  (sleep-santa)
  (dosync
    (let [reindeers (deref reindeer-queue-ref)
          elfs (deref elf-queue-ref)]
      (if (== (count reindeers) 9)
        (work-santa reindeers reindeer-queue-ref
                    "delivering presents with reindeers:")
        (work-santa elfs elf-queue-ref "helping elfs:"))))
  (recur))

;; Main
;;;;;;;
(defn main []
  (dosync
    (ref-set santa-thread-ref (Thread. santa))
    (.start (deref santa-thread-ref)))
  (doseq [id (range 30)]
    (future (worker id 15 elf-queue-ref 3))
    (doseq [id (range 9)]
      (future (worker id 30 reindeer-queue-ref 9)))))
(main)
(shutdown-agents)
