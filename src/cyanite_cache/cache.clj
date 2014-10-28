(ns cyanite-cache.cache
  "Caching facility for Cyanite"
  (:require [clojure.string :as str]
            [clojure.core.cache :as cache]))

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
                (fn-agg (fn-get (construct-key mkey path))) ttl))
    (swap! mkeys (fn [mkeys] (dissoc mkeys mkey)))))

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
           (fn [at-mkeys]
             (if (contains? at-mkeys mkey)
               at-mkeys
               (let [pkeys (atom #{})
                     flusher (create-flusher mkeys mkey pkeys tenant period
                                             rollup time ttl fn-get fn-agg
                                             fn-store)
                     delayer (run-delayer! rollup flusher)]
                 (swap! pkeys
                        (fn [pkeys]
                          (with-meta pkeys {:flusher flusher :delayer delayer})))
                 (assoc at-mkeys mkey pkeys)))))
    (get @mkeys mkey)))

(defn set-keys!
  [mkeys tenant period rollup time path ttl fn-get fn-agg fn-store]
  (let [pkeys (set-pkeys! mkeys tenant period rollup time ttl fn-get
                          fn-agg fn-store)]
    (swap! pkeys
           (fn [pkeys]
             (if (contains? pkeys path)
               pkeys
               (conj pkeys path))))))

(defn simple-cache
  [fn-store & {:keys [fn-agg] :or {fn-agg agg-avg}}]
  (let [caches (atom {})
        get-fns (atom {})
        mkeys (atom {})
        get-cache!
        (fn [rollup]
          (swap! caches
                 (fn [caches]
                   (if (contains? caches rollup)
                     caches
                     (let [cache-ttl (to-ms (calc-delay rollup
                                                        cache-add-ttl))]
                       (assoc caches rollup
                              (atom (cache/ttl-cache-factory
                                     {} :ttl cache-ttl)))))))
          (get @caches rollup))
        create-fn-get
        (fn [cache]
          (fn [key]
            (cache/lookup @cache key)))
        get-get-fn!
        (fn [rollup]
          (swap! get-fns
                 (fn [get-fns]
                   (if (contains? get-fns rollup)
                     get-fns
                     (assoc get-fns rollup
                            (create-fn-get (get-cache! rollup))))))
          (get @get-fns rollup))]
    (reify
      StoreCache
      (put! [this tenant period rollup time path data ttl]
        (let [ckey (construct-key tenant period rollup time path)]
          (set-keys! mkeys tenant period rollup time path ttl
                     (get-get-fn! rollup) fn-agg fn-store)
          (swap! (get-cache! rollup)
                 (fn [cache]
                   (assoc cache ckey (conj (cache/lookup cache ckey) data))))))
      (flush! [this]
        (doseq [[mkey pkeys] @mkeys]
          (let [delayer (get (meta @pkeys) :delayer nil)]
            (when delayer
              (println delayer)
              (future-cancel delayer)))))
      (-show-keys [this] (println "MKeys:" mkeys))
      (-show-cache [this] (println "Caches:" caches))
      (-show-meta
        [this]
        (doseq [[mkey pkeys] @mkeys]
          (println "Meta:" mkey (meta @pkeys)))))))
