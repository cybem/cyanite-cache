(ns cyanite-cache.cache
  "Caching facility for Cyanite"
  (:require [clojure.string :as str]
            [clojure.core.cache :as cache]))

(def wait-time 60)

(defprotocol StoreCache
  (push! [this tenant period rollup time path data ttl])
  (flush! [this]))

(defn construct-mkey
  [tenant period rollup]
  (str/join "-" [tenant period rollup]))

(defn get-tkeys!
  [mkeys tenant period rollup]
  (let [mkey (construct-mkey tenant period rollup)]
    (swap! mkeys
           (fn [mkeys]
             (if (contains? mkeys mkey)
               mkeys
               (assoc mkeys mkey (atom {})))))
    (get @mkeys mkey)))

(defn create-flusher
 [mkeys tkeys pkeys tenant period rollup time ttl fn-data fn-agg fn-store]
 (fn []
   (doseq [path @pkeys]
     (fn-store tenant period rollup time path (fn-agg (fn-data path)) ttl)
     (swap! tkeys (fn [tkeys] (dissoc tkeys time)))
     (when (empty? tkeys)
       (swap! mkeys (fn [mkeys mkey] (dissoc mkeys mkey))
              (construct-mkey tenant period rollup))))))

(defn run-delayer!
  [rollup flusher]
  (let [flush-delay (+ (int rollup) wait-time (rand-int 59))]
    (future
      (Thread/sleep (* flush-delay 1000))
      (flusher))))

(defn get-pkeys!
  [mkeys tkeys tenant period rollup time ttl fn-data fn-agg fn-store]
  (swap! tkeys
         (fn [tkeys time]
           (if (contains? tkeys time)
             tkeys
             (let [pkeys (atom [])
                   flusher (create-flusher mkeys tkeys pkeys tenant period
                                           rollup time ttl fn-data fn-agg
                                           fn-store)]
               (swap! pkeys
                      (fn [pkeys]
                        (with-meta pkeys {:flusher flusher})))
               (assoc! tkeys time pkeys)
               (run-delayer! rollup flusher))))
         time)
  (get @tkeys time))

(defn simple-cache
  []
  (defn fn-data [])
  (defn fn-agg [])
  (defn fn-store [])
  (let [mkeys (atom {})]
    (reify
      StoreCache
      (push! [this tenant period rollup time path data ttl]
        (let [tkeys (get-tkeys! mkeys tenant period rollup)
              pkeys (get-pkeys! mkeys tkeys tenant period rollup time ttl fn-data fn-agg fn-store)]
          )
        )
      )))
