(defproject yquant/stockdata "0.1.0-SNAPSHOT"
  :description "Fetch and store the stock information into Redis for providing all relevant subsystems of yQuant."
  :url "https://github.com/yoonbae81/yQuant.StockData"
  :license {:name "The MIT License"
            :url  "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.4.490"]
                 [org.clojure/data.json "0.2.6"]
                 [clj-http "3.9.1"]
                 [environ "1.1.0"]
                 [com.taoensso/carmine "2.19.1"]
                 [com.taoensso/timbre "4.10.0"]]
  ;  :main ^:skip-aot core
  :resource-paths ["resources"]
  :profiles
  {:repl    {:prep-tasks   ^:replace ["javac" "compile"]
             :repl-options {:init-ns core}}
   :uberjar {:aot :all}}
  )
