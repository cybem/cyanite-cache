(ns cyanite-cache.core
  (:gen-class)
  (:require [cyanite-cache.cache :as cache]))

(defn store
  [tenant period rollup time path data ttl]
  (println tenant period rollup time path data ttl))

(defn inspect
  [scache]
  (cache/-show-keys scache)
  (cache/-show-cache scache)
  (cache/-show-meta scache)
  (newline))

(defn -main
  "Main function."
  [& args]
  (let [scache (cache/simple-cache store)]
    (time (cache/put! scache "tenant" 123 60 117293 "my.metric" 10.0 600))
    (inspect scache)
    (time (cache/put! scache "tenant" 123 60 117293 "my.metric" 20.0 600))
    (inspect scache)
    (time (cache/put! scache "tenant" 123 60 117293 "my.metric" 30.0 600))
    (inspect scache)
    (Thread/sleep (* 10 1000))
    (inspect scache))
  (System/exit 0))
