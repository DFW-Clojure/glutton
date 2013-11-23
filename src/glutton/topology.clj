(ns glutton.topology
  "Topology

More info on the Clojure DSL here:

https://github.com/nathanmarz/storm/wiki/Clojure-DSL"
  (:require [glutton
             [spouts :refer [type-spout mock-twitter-spout]]
             [bolts :refer [stormy-bolt glutton-bolt sliding-count-bolt extract-hashtag-bolt]]]
            [backtype.storm [clojure :refer [topology spout-spec bolt-spec]] [config :refer :all]])
  (:import [backtype.storm LocalCluster LocalDRPC]))

(defn stormy-topology []
  (topology
   {"spout" (spout-spec mock-twitter-spout)}

   {
    ;"stormy-bolt" (bolt-spec {"spout" ["type"]} stormy-bolt :p 2)
    ;"glutton-bolt" (bolt-spec {"stormy-bolt" :shuffle} glutton-bolt :p 2)
    "hashtag-bolt" (bolt-spec {"spout" ["tweet"]} extract-hashtag-bolt :p 2)
    "sliding-count-bolt" (bolt-spec {"hashtag-bolt" ["hashtag"]} sliding-count-bolt)
    }))

(defn run! [& {debug "debug" workers "workers" :or {debug "true" workers "2"}}]
  (doto (LocalCluster.)
    (.submitTopology "stormy topology"
                     {TOPOLOGY-DEBUG (Boolean/parseBoolean debug)
                      TOPOLOGY-WORKERS (Integer/parseInt workers)}
                     (stormy-topology))))
