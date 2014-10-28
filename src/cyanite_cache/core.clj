(ns cyanite-cache.core
  (:gen-class)
  (:require [cyanite-cache.cache :as cache]))

(defn store [tenant period rollup time path data ttl]
  (println tenant period rollup time path data ttl))

(defn -main
  "Main function."
  [& args]
  (let [scache (cache/simple-cache store)]
    (time (cache/put! scache "tenant" 123 60 117293 "my.metric" 10.0 600))
    (cache/-show-keys scache)
    (cache/-show-cache scache)
    (time (cache/put! scache "tenant" 123 60 117293 "my.metric" 20.0 600))
    (cache/-show-keys scache)
    (cache/-show-cache scache)
    (time (cache/put! scache "tenant" 123 60 117293 "my.metric" 30.0 600))
    (cache/-show-keys scache)
    (cache/-show-cache scache))
  (Thread/sleep (* 190 1000))
  (System/exit 0))
