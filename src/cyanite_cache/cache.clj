(ns cyanite-cache.cache
  "Caching facility for Cyanite"
  (:require [clojure.string :as str]))

(defprotocol StoreCache
  (put! [this tenant period rollup time path data ttl])
  (flush! [this])
  (-show-keys [this])
  (-show-cache [this])
  (-show-meta [this]))

(def ^:const metric-wait-time 60)
(def ^:const cache-add-ttl 180)

(defn agg-avg
  [data]
  (/ (reduce + data) (count data)))

(defn construct-mkey
  [tenant period rollup time]
  (str/join "-" [tenant period rollup time]))

(defn construct-key
  ([tenant period rollup time path]
     (str/join "-" [(construct-mkey tenant period rollup time) path]))
  ([mkey path]
     (str/join "-" [mkey path])))

(defn calc-delay
  [rollup add]
  (+ rollup metric-wait-time add))

(defn to-ms
  [sec]
  (* sec 1000))

(defn create-flusher
  [mkeys mkey pkeys tenant period rollup time ttl fn-get fn-agg fn-store]
  (fn []
    (doseq [path @pkeys]
      (fn-store tenant period rollup time path
                (fn-agg (fn-get (construct-key mkey path) @pkeys)) ttl))
    (swap! mkeys #(dissoc % mkey))))

(defn run-delayer!
  [rollup flusher]
  (let [delayer (future
                  (Thread/sleep (to-ms (calc-delay rollup (rand-int 59)))))]
    (future
      (try
        (deref delayer)
        (catch Exception _))
      (flusher))
    delayer))

(defn set-pkeys!
  [mkeys tenant period rollup time ttl fn-get fn-agg fn-store]
  (let [mkey (construct-mkey tenant period rollup time)]
    (swap! mkeys
           #(if (contains? % mkey)
              %
              (let [pkeys (atom #{})
                    flusher (create-flusher mkeys mkey pkeys tenant period
                                            rollup time ttl fn-get fn-agg
                                            fn-store)
                    delayer (run-delayer! rollup flusher)]
                (swap! pkeys
                       (fn [pkeys]
                         (with-meta pkeys {:flusher flusher :delayer delayer
                                           :data (atom {})})))
                (assoc % mkey pkeys))))
    (get @mkeys mkey)))

(defn set-keys!
  [mkeys tenant period rollup time path ttl fn-get fn-agg fn-store]
  (let [pkeys (set-pkeys! mkeys tenant period rollup time ttl fn-get
                          fn-agg fn-store)]
    (swap! pkeys #(if (contains? % path) % (conj % path)))
    pkeys))

(defn simple-cache
  [fn-store & {:keys [fn-agg] :or {fn-agg agg-avg}}]
  (let [mkeys (atom {})
        get-data (fn [pkeys] (get (meta pkeys) :data))
        fn-get (fn [key pkeys] (get @(get-data pkeys) key))]
    (reify
      StoreCache
      (put! [this tenant period rollup time path data ttl]
        (let [ckey (construct-key tenant period rollup time path)
              pkeys (set-keys! mkeys tenant period rollup time path ttl fn-get
                               fn-agg fn-store)
              adata (get-data @pkeys)]
          (swap! adata #(assoc % ckey (conj (get % ckey) data)))))
      (flush! [this]
        (doseq [[mkey pkeys] @mkeys]
          (let [delayer (get (meta @pkeys) :delayer nil)]
            (when delayer
              (future-cancel delayer)))))
      (-show-keys [this] (println "MKeys:" mkeys))
      (-show-cache [this]
        (println "Cache:")
        (doseq [[mkey pkeys] @mkeys]
          (println (get-data @pkeys))))
      (-show-meta [this]
        (doseq [[mkey pkeys] @mkeys]
          (println "Meta:" mkey (meta @pkeys)))))))
