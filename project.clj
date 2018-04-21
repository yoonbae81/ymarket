(defproject yquant/stockdata "0.1.0-SNAPSHOT"
  :description "Fetch and store the stock information into Redis for providing all relevant subsystems of yQuant."
  :url "https://github.com/yoonbae81/yQuant.StockData"
  :license {:name "The MIT License"
            :url  "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.clojure/data.csv "0.1.4"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.6"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [clj-http "3.8.0"]
                 [com.taoensso/timbre "4.10.0"]
                 [com.taoensso/carmine "2.18.0"]
                 [environ "1.1.0"]
                 ]
  :main ^:skip-aot core
  :resource-paths ["resources" "target/resources"]
  :profiles
  {:repl    {:prep-tasks   ^:replace ["javac" "compile"]
             :repl-options {:init-ns core}}
   :uberjar {:aot :all}}
  )
