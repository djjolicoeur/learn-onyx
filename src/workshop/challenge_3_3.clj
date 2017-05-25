(ns workshop.challenge-3-3
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :prepend-tilde]
   [:prepend-tilde :append-question-mark]
   [:append-question-mark :write-segments]])

;;; Catalogs ;;;

(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [{:onyx/name :read-segments
       :onyx/plugin :onyx.plugin.core-async/input
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}

      ;; <<< BEGIN FILL ME IN PART 1 >>>

      {:onyx/name :prepend-tilde
       :onyx/fn ::bookend-char
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       ::bookend-char.op :prepend
       ::bookend-char.char "~"
       :onyx/params [::bookend-char.op ::bookend-char.char]
       :onyx/doc "prepend ~ to the segment name"}

      {:onyx/name :append-question-mark
       :onyx/fn ::bookend-char
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       ::bookend-char.op :append
       ::bookend-char.char "?"
       :onyx/params [::bookend-char.op ::bookend-char.char]
       :onyx/doc "append ? to the segment name"}

      ;; <<< END FILL ME IN PART 1 >>>

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

;; <<< BEGIN FILL ME IN PART 2 >>>

(defn bookend-char [op char segment]
  (condp = op
    :prepend (update segment :name #(apply str (cons char %)))
    :append (update segment :name #(apply str (cons % char)))
    (throw (ex-info "Invalid operator!"
                    {:causes #{:invalid-op}
                     :valid-ops #{:append :prepend}
                     :op op}))))

;; <<< END FILL ME IN  PART 2 >>>

;;; Lifecycles ;;;

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-3-3/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])
