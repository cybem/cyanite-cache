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

(defn calc-delay
  [rollup add]
  (+ (int rollup) wait-time add))

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
                          fn-agg fn-store)]
    (when-not (contains? pkeys path)
      (swap! pkeys conj path))))

(defn simple-cache
  [max-rollup]
  (defn fn-agg [data])
  (defn fn-store [data])
  (let [cache-ttl (* (calc-delay max-rollup 120) 1000)
        ccache (atom (cache/ttl-cache-factory {} :ttl cache-ttl))
        mkeys (atom {})]
    (defn fn-get [path])
    (reify
      StoreCache
      (push! [this tenant period rollup time path data ttl]
        (set-keys! mkeys tenant period rollup time path ttl fn-get fn-agg fn-store)
        )
      )))
