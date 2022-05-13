import os
import io
import json
import argparse
import time
import concurrent.futures
import ntpath
import requests
import pathlib
import gzip


class ServerUnavailableException(Exception):
    pass


class DataseerClient:
    def __init__(self, config_path=None):
        self.endpoint_isalive = "isalive"
        self.endpoint_tei = "processDataseerTEI"
        self.endpoint_pdf = "processDataseerPDF"
        self._load_config(config_path)
        self.service_url = "http://" + self.config["dataseer_server"]
        if len(self.config["dataseer_port"]) > 0:
            self.service_url += ":" + self.config["dataseer_port"]
        self.service_url += "/service/"

        self._test_server_connection()

    def _load_config(self, path="./config.json"):
        """Load the json configuration"""
        config_json = open(path).read()
        self.config = json.loads(config_json)

    def _test_server_connection(self):
        """Test if the server is up and running."""
        the_url = self.service_url + self.endpoint_isalive
        try:
            r = requests.get(the_url)
        except:
            print(
                "Dataseer server does not appear up and running, the connection to the server failed"
            )
            raise ServerUnavailableException

        status = r.status_code

        if status != 200:
            print("Dataseer server does not appear up and running " + str(status))
        else:
            print("Dataseer server is up and running")

    def _output_file_name(self, input_file, input_path, output):
        # we use ntpath here to be sure it will work on Windows too
        if output is not None:
            input_file_name = str(os.path.relpath(os.path.abspath(input_file), input_path))
            filename = os.path.join(
                output, os.path.basename(input_file_name).split(".")[0] + ".dataseer.tei.xml"
            )
        else:
            input_file_name = ntpath.basename(input_file)
            filename = os.path.join(
                ntpath.dirname(input_file),
                os.path.basename(input_file_name).split(".")[0] + ".dataseer.tei.xml",
            )

        return filename

    def process(self, service, input_path, output=None, n=10, force=True, verbose=False):
        start_time = time.time()
        nb_total = 0
        print(f"\nDataseer - total process: {nb_total} - accumulated runtime: 0 s - 0 PDF/s\n")

        batch_size_files = self.config["batch_size"]
        input_files = []

        for (dirpath, dirnames, filenames) in os.walk(input_path):
            for filename in filenames:
                if (
                    service == "processDataseerPDF"
                    and (filename.endswith(".pdf.gz") or filename.endswith(".pdf"))
                    ) or (
                    service == "processDataseerTEI" and filename.endswith(".tei.xml")
                ):
                    if verbose:
                        try:
                            print(f"Dataseer - {filename}")
                        except Exception:
                            # may happen on linux see https://stackoverflow.com/questions/27366479/python-3-os-walk-file-paths-unicodeencodeerror-utf-8-codec-cant-encode-s
                            pass
                    input_files.append(os.sep.join([dirpath, filename]))

                    if len(input_files) == batch_size_files:
                        self.process_batch(
                            service,
                            input_files,
                            input_path,
                            output,
                            n,
                            force,
                            verbose,
                        )
                        runtime = round(time.time() - start_time, 3)
                        nb_total += len(input_files)
                        print(
                            f"\nDataseer - total process: {nb_total} - accumulated runtime: {runtime}s - {round(nb_total/runtime, 2)} PDF/s\n"
                        )
                        input_files = []

        # last batch
        if len(input_files) > 0:
            self.process_batch(
                service,
                input_files,
                input_path,
                output,
                n,
                force,
                verbose,
            )

    def process_batch(self, service, input_files, input_path, output, n, force, verbose=False):
        if verbose:
            print(f"\nDataseer - {len(input_files)} files to process in current batch\n")

        # with concurrent.futures.ThreadPoolExecutor(max_workers=n) as executor:
        with concurrent.futures.ProcessPoolExecutor(max_workers=n) as executor:
            results = []
            for input_file in input_files:
                # check if TEI file is already produced
                filename = self._output_file_name(input_file, input_path, output)
                if not force and os.path.isfile(filename):
                    print(
                        f"Dataseer - {filename} already exist, skipping... (use --force to reprocess pdf input files)"
                    )
                    continue

                selected_process = self.process_tei
                if service == self.endpoint_pdf:
                    selected_process = self.process_pdf

                r = executor.submit(selected_process, service, input_file)

                results.append(r)

        for r in concurrent.futures.as_completed(results):
            input_file, status, text = r.result()
            filename = self._output_file_name(input_file, input_path, output)

            if text is None:
                print(f"Dataseer - Processing of {input_file} failed with error {status}")
            else:
                # writing TEI file
                try:
                    pathlib.Path(os.path.dirname(filename)).mkdir(parents=True, exist_ok=True)
                    with open(filename, "w", encoding="utf8") as tei_file:
                        tei_file.write(text)
                except OSError:
                    print(f"Dataseer - Writing resulting TEI XML file {filename} failed")

    def process_pdf(self, service, pdf_file):
        url = self.service_url + self.endpoint_pdf
        if pdf_file.endswith('.pdf.gz'):
            the_file = {'input': gzip.open(pdf_file, 'rb')}
        else:
            the_file = {'input': open(pdf_file, 'rb')}
        try:
            response = requests.post(url, files=the_file, timeout=self.config["timeout"])
            tei_data = None
            if response.status_code == 503:
                print(
                    "Info: service overloaded, sleep " + str(self.config["sleep_time"]) + " seconds"
                )
                time.sleep(self.config["sleep_time"])
                return self.process_tei(service, pdf_file)
            elif response.status_code >= 500:
                print("Error: [{0}] Server Error ".format(response.status_code) + pdf_file)
            elif response.status_code == 404:
                print("Error: [{0}] URL not found: [{1}] ".format(response.status_code + url))
            elif response.status_code >= 400:
                print("Error: [{0}] Bad Request".format(response.status_code))
                print("Error: ", response.content)
            elif response.status_code == 200:
                tei_data = response.text
                # note: in case the recognizer has found no software in the document, it will still return
                # a json object as result, without mentions, but with MD5 and page information
            else:
                print(
                    "Error: Unexpected Error: [HTTP {0}]: Content: {1}".format(
                        response.status_code, response.content
                    )
                )

        except requests.exceptions.Timeout:
            print("Exception:  The request to the annotation service has timeout")
        except requests.exceptions.TooManyRedirects:
            print("Exception:  The request failed due to too many redirects")
        except requests.exceptions.RequestException:
            print("Exception:  The request failed")
        finally:
            return (pdf_file, response.status_code, tei_data)

    def process_tei(self, service, tei_file):
        url = self.service_url + self.endpoint_tei
        the_file = {'input': open(tei_file, 'rb')}
        try:
            response = requests.post(url, files=the_file, timeout=self.config["timeout"])
            tei_data = None
            if response.status_code == 503:
                print(
                    "Info: service overloaded, sleep " + str(self.config["sleep_time"]) + " seconds"
                )
                time.sleep(self.config["sleep_time"])
                return self.process_tei(service, tei_file)
            elif response.status_code >= 500:
                print("Error: [{0}] Server Error ".format(response.status_code) + tei_file)
            elif response.status_code == 404:
                print("Error: [{0}] URL not found: [{1}] ".format(response.status_code + url))
            elif response.status_code >= 400:
                print("Error: [{0}] Bad Request".format(response.status_code))
                print("Error: ", response.content)
            elif response.status_code == 200:
                tei_data = response.text
                # note: in case the recognizer has found no software in the document, it will still return
                # a json object as result, without mentions, but with MD5 and page information
            else:
                print(
                    "Error: Unexpected Error: [HTTP {0}]: Content: {1}".format(
                        response.status_code, response.content
                    )
                )

        except requests.exceptions.Timeout:
            print("Exception:  The request to the annotation service has timeout")
        except requests.exceptions.TooManyRedirects:
            print("Exception:  The request failed due to too many redirects")
        except requests.exceptions.RequestException:
            print("Exception:  The request failed")
        finally:
            return (tei_file, response.status_code, tei_data)
