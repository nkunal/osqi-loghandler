(ns osqi_loghandler.core
  (:require [clojure.string :as string]
            [clojure.pprint :as pprint]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :refer (parse-opts)]
            [compojure.core :refer [defroutes POST]]
            [ring.util.response :refer [resource-response response]]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.middleware.json :refer [wrap-json-response wrap-json-body wrap-json-params]]
            [ring.adapter.jetty :as jetty]
            [cheshire.core :as json]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [clj-kafka.producer :as kproducer]
            [clj-kafka.core :as kafkacore])
  (:gen-class))

(def config-props (atom {}))
(def kafka-producer (atom nil))

(defn process-post
  [req]
  (let [bodystr (slurp (:body req))
        json-body (json/parse-string bodystr)
        data-json (json-body "data")
        log-type (json-body "log_type")
        topic (get @config-props :topic.name)]
    (if (= log-type "status")
      (prn "Kunal process-post received STATUS")
      (do
        (doseq [item data-json]
          (pprint/pprint (str "unixTime=" (item "unixTime")
                              ", name=" (item "name")
                              ", action=" (item "action")
                              ", columns=" (item "columns"))))
        (kproducer/send-message @kafka-producer (kproducer/message topic (.getBytes bodystr)))))))

(defroutes app-routes
  (POST "/agent/log" req
        (process-post req)
        (response {:node_invalid false}))
  (route/not-found "Page not found"))

(defn wrap-correct-content-type [handler]
  (fn [request]
    (handler (assoc
              (assoc-in request [:headers "content-type"] "application/json")
              :content-type "application/json"))))

(def app
  (-> app-routes
      wrap-json-response))

(defn run-main
  [cprops]
  (jetty/run-jetty (handler/site app) {:port (get cprops :port)}))

(def cli-options
  [["-c" "--config-file CONFIG-FILE" "Config file "]
   ["-h" "--help"]])

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn  load-resource
  [config-file]
  (let [thr (Thread/currentThread)
        ldr (.getContextClassLoader thr)]
    (read-string (slurp (.getResourceAsStream ldr config-file)))))

(defn -main
  "main func "
  [& args]
  (let [{:keys [options arguments summary errors]} (parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 summary)
      (not (:config-file options)) (exit 1 (str "config-file not passed usage=" summary))
      errors (exit 2 error-msg errors))
    (let [{:keys [config-file]} options
          cprops (load-resource config-file)
          broker-list (get cprops :broker.list)
          prods (kproducer/producer {"metadata.broker.list" broker-list
                                     "serializer.class" "kafka.serializer.DefaultEncoder"
                                     "partitioner.class" "kafka.producer.DefaultPartitioner"})]
      (log/info "running with config-file=" config-file " edn.data=" cprops)
      (try
        (reset! config-props cprops)
        (reset! kafka-producer prods)
        (run-main cprops)
        (catch Exception e
          (do
            (.printStackTrace e)
            (System/exit 2)))))))
