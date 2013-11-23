(ns glutton.bolts
  "Bolts.

More info on the Clojure DSL here:

https://github.com/nathanmarz/storm/wiki/Clojure-DSL"
  (:require [backtype.storm [clojure :refer [emit-bolt! defbolt ack! bolt]]])
  (:import [backtype.storm Constants]))

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

; the java example does this map<word, counts[]>
; this does map<slots, map<words, count>> , which makes sliding the window much simpler
(defn bump-count! [counts word slot]
  (swap! counts
         (fn [p] (update-in p [slot word] (fnil inc 0)))))

(defn top-n [counts n]
  (let [word-counts (apply (partial merge-with +) (vals counts)) ; merge all the slots
        ordered-counts (reverse (sort-by second (vec word-counts)))] ; sort by most popular words
    (map first (take n ordered-counts)))) ; get the top n most popular

; replaced by the builtin tick spout
;(defn current-slot []
;  (mod (rem (System/currentTimeMillis) 1000) WINDOW_SIZE_SEC))

; global state, not serializable!
;(def counts (atom {}))

; from storm-starter/src/jvm/storm/starter/util/TupleHelpers.java
(defn is-tick [tuple]
  (= (.getSourceStreamId tuple) (Constants/SYSTEM_TICK_STREAM_ID)))

; prepared bolt see https://github.com/nathanmarz/storm/wiki/Clojure-DSL#prepared-bolts
(defbolt sliding-count-bolt ["counts"]
  {:prepare true
   ; https://groups.google.com/forum/#!topic/storm-user/9M6O2fo0ugM
   ; sends this bolt a 'tick' tuple every sec
   :conf {"topology.tick.tuple.freq.secs", 1}}
  [config context collector]
  (let [counts (atom {})
        slot (atom 0)]
    (bolt
      (execute [tuple]
               (if (is-tick tuple)
                 (do ; dosync?
                   (swap! slot (fn [s] (mod (inc s) WINDOW_SIZE_SEC)))
                   (swap! counts dissoc @slot)
                   (emit-bolt! collector [(top-n @counts 3)] :anchor tuple))
                 (let [tag (:hashtag tuple)] ; NOT (tuple :hashtag)
                   (bump-count! counts tag @slot)
;                   (emit-bolt! collector [@counts] :anchor tuple)
                   (ack! collector tuple)))))))
