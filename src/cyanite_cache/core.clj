(ns cyanite-cache.core
  (:gen-class)
  (:require [cyanite-cache.cache :as cache]))

(def store-count (atom 0))

(defn store
  [tenant period rollup time path data ttl]
  ;;(println tenant period rollup time path data ttl)
  (swap! store-count inc))

(defn inspect
  [scache]
  (cache/-show-keys scache)
  (cache/-show-cache scache)
  (cache/-show-meta scache)
  (newline))

(defn -main
  "Main function."
  [& args]
  (let [scache (cache/simple-cache store)
        num-inserts 1000000
        num-metrics 10000]
    (time
     (doall (doseq [_ (range num-inserts)]
              (cache/put! scache "tenant" 123 60 117293
                          (format "my.metric%s" (rand-int num-metrics))
                          (rand 100) 600))))
    (println (format "%s values is in the cache" num-inserts))
    ;;(inspect scache)
    (Thread/sleep (* 10 1000))
    (cache/flush! scache)
    (Thread/sleep (* 10 1000))
    (println "Store count:" @store-count)
    ;;(Thread/sleep (* 190 1000))
    ;;(inspect scache)
    )
  (System/exit 0))
