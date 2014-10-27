(ns cyanite-cache.cache
  "Caching facility for Cyanite"
  (:require [clojure.string :as str]
            [clojure.core.cache :as cache]))

(defprotocol StoreCache
  (push! [this tenant period rollup time path data ttl])
  (flush! [this]))

(def ^:const metric-wait-time 60)
(def ^:const cache-add-ttl 180)

(defn agg-avg
  [data]
  (/ (reduce + data) (count data)))

(defn construct-mkey
  [tenant period rollup]
  (str/join "-" [tenant period rollup]))

(defn calc-delay
  [rollup add]
  (+ (int rollup) metric-wait-time add))

(defn to-ms
  [sec]
  (* sec 1000))

(defn set-tkeys!
  [mkeys tenant period rollup]
  (let [mkey (construct-mkey tenant period rollup)]
    (swap! mkeys
           (fn [mkeys]
             (if (contains? mkeys mkey)
               mkeys
               (assoc mkeys mkey (atom {})))))
    (get @mkeys mkey)))

(defn create-flusher
 [mkeys tkeys pkeys tenant period rollup time ttl fn-get fn-agg fn-store]
 (fn []
   (doseq [path @pkeys]
     (fn-store tenant period rollup time path (fn-agg (fn-get path)) ttl)
     (swap! tkeys (fn [tkeys] (dissoc tkeys time)))
     (when (empty? tkeys)
       (swap! mkeys (fn [mkeys mkey] (dissoc mkeys mkey))
              (construct-mkey tenant period rollup))))))

(defn run-delayer!
  [rollup flusher]
  (let [delayer (future
                  (Thread/sleep (* (calc-delay rollup (rand-int 59)) 1000)))]
    (future
      (deref delayer)
      (flusher))
    delayer))

(defn set-pkeys!
  [mkeys tkeys tenant period rollup time ttl fn-get fn-agg fn-store]
  (swap! tkeys
         (fn [tkeys time]
           (if (contains? tkeys time)
             tkeys
             (let [pkeys (atom [])
                   flusher (create-flusher mkeys tkeys pkeys tenant period
                                           rollup time ttl fn-get fn-agg
                                           fn-store)
                   delayer (run-delayer! rollup flusher)]
               (swap! pkeys
                      (fn [pkeys]
                        (with-meta pkeys {:flusher flusher :delayer delayer})))
               (assoc! tkeys time pkeys))))
         time)
  (get @tkeys time))

(defn set-keys!
  [mkeys tenant period rollup time path ttl fn-get fn-agg fn-store]
  (let [tkeys (set-tkeys! mkeys tenant period rollup)
        pkeys (set-pkeys! mkeys tkeys tenant period rollup time ttl fn-get
                          fn-agg fn-store)
        exists (contains? pkeys path)]
    (when-not exists
      (swap! pkeys conj path))
    exists))

(defn simple-cache
  [fn-agg fn-store]
  (let [caches (atom {})
        get-fns (atom {})
        mkeys (atom {})
        get-cache
        (fn [rollup]
          (let [rollup (int rollup)]
            (swap! caches
                   (fn [caches]
                     (if (contains? caches rollup)
                       caches
                       (let [cache-ttl (to-ms (calc-delay rollup
                                                          cache-add-ttl))]
                         (assoc caches rollup (cache/ttl-cache-factory
                                               {} :ttl cache-ttl)))))))
          (get @caches rollup))
        create-fn-get
        (fn [cache]
          (fn [key]))
        get-get-fn
        (fn [rollup]
          (let [rollup (int rollup)]
            (swap! get-fns
                   (fn [get-fns]
                     (if (contains? get-fns rollup)
                       get-fns
                       (assoc get-fns rollup
                              (create-fn-get (get-cache rollup)))))))
          (get @get-fns rollup))]
    (reify
      StoreCache
      (push! [this tenant period rollup time path data ttl]
        (set-keys! mkeys tenant period rollup time path ttl (get-get-fn rollup) fn-agg fn-store)
        )
      )))
