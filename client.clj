#!/usr/bin/env bb

(require
 '[honey.sql :as sql]
 '[netpod.pods :as pods])

(def contact-ins
  {:insert-into :contacts
   :values [{:id :?id
             :first_name :?first_name
             :last_name :?last_name
             :email :?email}]})

(def all-q {:select [:*]
            :from [:contacts]})

(def signals-q
  {:select [[[:count "*"] :count] :sensor_name]
   :from [:signals]
   :group-by [:sensor_name]
   :order-by [[:count :desc]]})

(def domain-q {:select [:*]
               :from [:contacts]
               :where [:like :email :?domain]})

(defn to-sql
  "uses honeysql to generate an sql string"
  ([sql-map params]
   (-> (sql/format sql-map {:inline true
                            :params params})
       first))
  ([sql-map]
   (to-sql sql-map nil)))

(defn random-item
  [coll]
  (let [coll (vec coll)
        size (count coll)
        idx (rand-int size)]
    (get coll idx)))

(defn random-signal
  []
  (let [uuid (str (random-uuid))
        sensor-name (random-item ["wheezy" "chewy" "poky" "smooth" "chill" "touchy"])
        gauge (rand-int 20000)]
    (hash-map :uuid uuid :sensor-name sensor-name :gauge gauge)))

(def signal-ins
  {:insert-into :signals
   :values [{:signal_id :?uuid
             :sensor_name :?sensor-name
             :gauge :?gauge}]})

;;before running do
;;cargo build --release
;;to build the binary
(pods/with-pod "./target/release/netpod-jlabath-sqlite"
  ;; require is not suitable in macros
  ;; but one can also resolve things dynamically using resolve such as below
  (let [query (resolve 'netpod.jlabath.sqlite/query)
        exec (resolve 'netpod.jlabath.sqlite/exec)
        ins (comp deref exec (partial to-sql signal-ins) random-signal)]
    (println @(exec "CREATE TABLE contacts(id BIGINT, first_name VARCHAR, last_name VARCHAR, email VARCHAR)"))
    (println @(exec (to-sql contact-ins {:id 1
                                         :first_name "John"
                                         :last_name "Doe"
                                         :email "john@doe.com"})))
    (println @(exec (to-sql contact-ins {:id 2
                                         :first_name "Pete"
                                         :last_name "Stolli"
                                         :email "petes@gmail.com"})))
    (println @(exec (to-sql contact-ins {:id 3
                                         :first_name "Mike"
                                         :last_name "Freight"
                                         :email "mf@gmail.com"})))
    (println @(query (to-sql domain-q {:domain "%@test.com"})))
    (println @(query (to-sql domain-q {:domain "%@gmail.com"})))
    (println @(query (to-sql domain-q {:domain "%@yahoo.com"})))
    (println @(query (to-sql domain-q {:domain "%@doe.com"})))
    (println @(query (to-sql domain-q {:domain "%@hotmail.com"})))
    (println @(exec "CREATE TABLE signals (signal_id varchar(255), sensor_name VARCHAR(255), gauge INT)"))
    (doall (repeatedly 1000 ins))
    (println @(query (to-sql signals-q)))
    (doall (repeatedly 5000 ins))
    (println @(query (to-sql signals-q)))
    (println @(exec "DROP TABLE contacts"))
    (println @(exec "DROP TABLE signals"))))

