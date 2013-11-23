(ns glutton.bolts
  "Bolts.

More info on the Clojure DSL here:

https://github.com/nathanmarz/storm/wiki/Clojure-DSL"
  (:require [backtype.storm [clojure :refer [emit-bolt! defbolt ack! bolt]]]))

(defbolt stormy-bolt ["stormy"] [{type :type :as tuple} collector]
  (emit-bolt! collector [(case type
                           :regular "I'm regular Stormy!"
                           :bizarro "I'm bizarro Stormy!"
                           "I have no idea what I'm doing.")]
              :anchor tuple)
  (ack! collector tuple))

(defbolt extract-hashtag-bolt ["hashtag"] [{tweet :tweet :as tuple} collector]
  (let [hashtags (map second (re-seq #"\#(\w\w+)" (str tuple)))]
    (emit-bolt! collector [hashtags] :anchor tuple))
  (ack! collector tuple))

(defbolt glutton-bolt ["message"] [{stormy :stormy :as tuple} collector]
  (emit-bolt! collector [(str "glutton produced: "stormy)] :anchor tuple)
  (ack! collector tuple))
