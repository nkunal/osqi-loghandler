(defproject osqi-loghandler "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [clj-kafka "0.2.8-0.8.1.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [cheshire "5.4.0"]
                 [clj-http "1.1.0"]
                 [clj-time "0.6.0"]
                 [compojure "1.5.1"]
                 [ring/ring-jetty-adapter "1.3.2"]
                 [ring/ring-json "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.10"]]
  :main ^:skip-aot osqi-loghandler.core
  :plugins [[brightnorth/uberjar-deploy "1.0.1" ]]
  :profiles {:uberjar {:aot :all}}
  :min-lein-version "2.5.0")
