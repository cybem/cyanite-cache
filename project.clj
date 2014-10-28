(defproject cyanite-cache "0.1.0"
  :description "Caching facility and store middleware for Cyanite"
  :url "https://github.com/cybem/cyanite-cache"
  :license {:name "MIT"
            :url "https://github.com/cybem/cyanite-cache/LICENSE"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.cli "0.3.1"]]
  :main ^:skip-aot cyanite-cache.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
