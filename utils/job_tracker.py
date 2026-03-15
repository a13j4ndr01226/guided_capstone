import datetime
import psycopg2


class Tracker(object):
    """
    Tracks job execution status in PostgreSQL.

    Table schema:
        job_id
        status
        updated_time
    """

    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig

    def assign_job_id(self):
        today = datetime.date.today().strftime("%Y-%m-%d")
        job_id = f"{self.jobname}_{today}"
        return job_id

    def get_db_connection(self):
        connection = None
        try:
            connection = psycopg2.connect(
                host=self.dbconfig.get("postgres", "host"),
                database=self.dbconfig.get("postgres", "database"),
                user=self.dbconfig.get("postgres", "user"),
                password=self.dbconfig.get("postgres", "password"),
                port=self.dbconfig.get("postgres", "port"),
            )
        except Exception as error:
            print("Error while connecting to PostgreSQL", error)

        return connection

    def update_job_status(self, status):
        job_id = self.assign_job_id()
        print(f"Job ID Assigned: {job_id}")

        update_time = datetime.datetime.now()

        table_name = self.dbconfig.get(
            "postgres", "job_tracker_table_name"
        )

        connection = self.get_db_connection()

        try:
            cursor = connection.cursor()

            query = f"""
            INSERT INTO {table_name} (job_id, status, updated_time)
            VALUES (%s, %s, %s)
            ON CONFLICT (job_id)
            DO UPDATE SET
                status = EXCLUDED.status,
                updated_time = EXCLUDED.updated_time
            """

            cursor.execute(query, (job_id, status, update_time))

            connection.commit()
            cursor.close()
            connection.close()

        except (Exception, psycopg2.Error) as error:
            print("error executing db statement for job tracker.", error)

    def get_job_status(self, job_id):
        table_name = self.dbconfig.get("postgres", "job_tracker_table_name")

        connection = self.get_db_connection()

        try:
            cursor = connection.cursor()

            query = f"""
            SELECT job_id, status, updated_time
            FROM {table_name}
            WHERE job_id = %s
            """

            cursor.execute(query, (job_id,))
            record = cursor.fetchone()

            cursor.close()
            connection.close()

            return record

        except (Exception, psycopg2.Error) as error:
            print("error executing db statement for job tracker.", error)
            return None