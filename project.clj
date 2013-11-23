(defproject glutton "0.1.0-SNAPSHOT"
  :description "Data ingestion for the lambda architecture."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [twitter-api "0.7.4"]]
  ;:aot [glutton.TopologySubmitter]
  ;; include storm dependency only in dev because production storm cluster provides it
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[storm "0.8.1"]
                                  [org.clojure/tools.namespace "0.2.3"]
                                  [org.clojure/java.classpath "0.2.0"]]}})
