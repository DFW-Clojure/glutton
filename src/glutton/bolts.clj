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

;; below is untested

(def ^:const WINDOW_SIZE_SEC 10)

(defn bump-count! [counts word slot]
  (swap! counts
         (fn [p] (update-in p [word slot] (fnil inc 0)))))

(defn current-slot []
  (mod (rem (System/currentTimeMillis) 1000) WINDOW_SIZE_SEC))

(def counts (atom {}))

; Matt's original
; (defbolt sliding-count-bolt ["counts"] [{tag :hashtag :as tuple} collector]
;   (let [counts (atom {})]
;     (bolt
;       (execute [tuple]
;                (let [word (tuple :hashtag)]
;                  (print word)
;                  (bump-count! counts word (current-slot))
;                  (emit-bolt! collector [@counts] :anchor tuple)
;                  (ack! collector tuple)
;                  )))))

(defbolt sliding-count-bolt ["counts"] [{tag :hashtag :as tuple} collector]
  (print tag)
  (bump-count! counts tag (current-slot))
  (emit-bolt! collector [@counts] :anchor tuple)
  (ack! collector tuple))
