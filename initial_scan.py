import simplejson
import json

from mrjob.job import MRJob
# from mrjob.launch import _READ_ARGS_FROM_SYS_ARGV
import sys
import codecs
import traceback
import mrjob

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)


def dict_to_sorted_tuple_list(dictx):
    if isinstance(dictx, dict):
        return [(key, dict_to_sorted_tuple_list(val))
                for key, val in sorted(dictx.items(), key=lambda item:item[0])]
    else:
        return dictx


def dict_to_sorted_str(dictx):
    if isinstance(dictx, dict):
        return [str(key) + dict_to_sorted_str(val) for key, val in sorted(dictx.items())]
    else:
        return str(dictx)


def get_date_prints_and_tiebreak_info(payload, job, fhr_version):
    # NOTE: we drop any packet without data.days entries. these cannot be
    # fingerprinted/linked.

    try:
        data_days_dates = payload["data"]["days"].keys()
    except:
        job.increment_counter("v%s MAP ERROR" % fhr_version, "no dataDaysDates")
        job.increment_counter("v%s MAP ERROR" % fhr_version, "REJECTED RECORDS")
        return (None, None)

    try:
        this_ping_date = payload["thisPingDate"]
    except:
        job.increment_counter("v%s MAP ERROR" % fhr_version, "no thisPingDate")
        job.increment_counter("v%s MAP ERROR" % fhr_version, "REJECTED RECORDS")
        return (None, None)

    if fhr_version == "3":
        try:
            current_env_hash = payload['environments']['current']['hash']
        except:
            job.increment_counter(
                "v%s MAP ERROR" % fhr_version, "without current env hash")
            job.increment_counter(
                "v%s MAP ERROR" % fhr_version, "REJECTED RECORDS")
            return (None, None)

    try:
        if fhr_version == "3":
            num_sessions = len(payload['data']['days'][this_ping_date][current_env_hash]['org.mozilla.appSessions'].get(
                'normal', [0])) + len(payload['data']['days'][this_ping_date][current_env_hash]['org.mozilla.appSessions'].get('abnormal', []))
        elif fhr_version == "2":
            num_sessions = len(
                payload["data"]["days"][this_ping_date]['org.mozilla.appSessions.previous']["main"])
    except:
        job.increment_counter(
            "v%s MAP WARNING" % fhr_version, "no num_sessions")
        num_sessions = 0

    try:
        if fhr_version == "3":
            current_session_time = payload['data']['days'][this_ping_date][
                current_env_hash]['org.mozilla.appSessions'].get('normal', [0])[-1]["d"]
        elif fhr_version == "2":
            current_session_time = payload["data"]["last"][
                'org.mozilla.appSessions.current']["totalTime"]
    except KeyError:
        current_session_time = 0
        job.increment_counter(
            "v%s MAP WARNING" % fhr_version, "no currentSessionTime, KeyError")
    except TypeError:
        current_session_time = 0
        job.increment_counter(
            "v%s MAP WARNING" % fhr_version, "no currentSessionTime, TypeError")
    # NOTE: we will use profile creation date to add further refinement to
    # date collisions, but it is not required.

    try:
        if fhr_version == "3":
            profile_creation = payload['environments']['current'][
                'org.mozilla.profile.age']['profileCreation']
        elif fhr_version == "2":
            profile_creation = payload['data']['last'][
                'org.mozilla.profile.age']['profileCreation']
    except KeyError:
        profile_creation = "00000"
        job.increment_counter(
            "v%s MAP WARNING" % fhr_version, "no profileCreation")

    date_prints = []
    for date in data_days_dates:
        try:
            # was getting  "AttributeError: 'float' object has no attribute
            # 'keys'"
            if fhr_version == "3":
                date_prints.append(str(profile_creation) + "_" + date + "_" + str(
                    hash(simplejson.dumps(payload["data"]["days"][date], sort_keys=True))))
            elif fhr_version == "2":
                if 'org.mozilla.appSessions.previous' in payload["data"]["days"][date].keys():
                    date_prints.append(str(profile_creation) + "_" + date + "_" + str(
                        hash(simplejson.dumps(payload["data"]["days"][date], sort_keys=True))))
        except:
            job.increment_counter(
                "v%s MAP ERROR" % fhr_version, "bad datePrints")
            job.increment_counter(
                "v%s MAP ERROR" % fhr_version, "REJECTED RECORDS")
            return (None, None)

    # emit tieBreakInfo
    # job.increment_counter("MAPPER", "(docId,tieBreakInfo) out")
    tiebreak_info = "_".join(
        [this_ping_date, str(num_sessions), str(current_session_time)])

    return (date_prints, tiebreak_info)


class ScanJob(MRJob):
    HADOOP_INPUT_FORMAT = "org.apache.hadoop.mapred.SequenceFileAsTextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol
    HADOOP_OUTPUT_FORMAT = 'test.NaiveMultiOutputFormatTwo'

    def configure_options(self):
        super(ScanJob, self).configure_options()

        # self.add_passthrough_option('--start-date', help = "Specify start date",
        # default = datetime.now().strftime("%Y-%m-%d"))

    def mapper(self, key, strx):
        docid = key
        self.increment_counter("MAPPER", "INPUT (docId,payload)")

        try:
            payload = simplejson.loads(strx)
        except:
            # added this block because as a fall back:
            # simplejson was failing to parse ~70k records that jackson parsed
            # ok
            try:
                payload = json.loads(strx)
            except:
                # print >> sys.stderr, 'Exception (ignored)', sys.exc_info()[0], sys.exc_info()[1]
                # traceback.print_exc(file = sys.stderr)
                self.increment_counter("MAP ERROR", "record failed to parse")
                self.increment_counter("MAP ERROR", "REJECTED RECORDS")
                return

        try:
            fhr_version = str(payload["version"])
        except:
            print >> sys.stderr, 'Exception (ignored)', sys.exc_info()[
                0], sys.exc_info()[1]
            traceback.print_exc(file=sys.stderr)
            self.increment_counter("MAP ERROR", "no version")
            self.increment_counter("MAP ERROR", "REJECTED RECORDS")
            return

        if fhr_version == "2":
            self.increment_counter("v2 MAP INFO", "fhr v2 records")
            try:
                date_prints, tiebreak_info = get_date_prints_and_tiebreak_info(
                    payload, self, fhr_version)
            except:
                print >> sys.stderr, 'Exception (ignored)', sys.exc_info()[
                    0], sys.exc_info()[1]
                traceback.print_exc(file=sys.stderr)
                self.increment_counter(
                    "v2 MAP ERROR", "get_date_prints_and_tiebreak_info_v2 failed")
                return
        elif fhr_version == "3":
            # yield  "v"+fhr_version+"/kDocId_vPartId_init|"+docid, "p"+docid
            self.increment_counter("v3 MAP INFO", "fhr v3 records")
            date_prints, tiebreak_info = get_date_prints_and_tiebreak_info(
                payload, self, fhr_version)
        else:
            self.increment_counter("MAP ERROR", "fhr version not 2 or 3")
            self.increment_counter("MAP ERROR", "REJECTED RECORDS")
            return

        # yield  "v"+fhr_version+"/kDocId_vPartId_init|"+docid, "p"+docid
        # emit date_prints
        if tiebreak_info:
            if date_prints:
                for dp in date_prints:
                    self.increment_counter(
                        "v%s MAP INFO" % fhr_version, "v%s (datePrint;docId) out" % fhr_version)
                    yield "|".join(["kDatePrint_vDocId", fhr_version, dp]), docid
                # if there ARE datePrints, then the record IS linkable, so
                # we'll need to emit tiebreak_info
                self.increment_counter(
                    "v%s MAP INFO" % fhr_version, "v%s (docId;tieBreakInfo) out" % fhr_version)
                yield "|".join(["kDocId_vTieBreakInfo", fhr_version, docid]), tiebreak_info
                # "PASSv"+fhr_version+"/kDocId_vTieBreakInfo|"+docId,  tiebreak_info
            else:
                # NOTE: if there are NO date prints in a record, the record
                # cannot be linked to any others. pass it through with it's own
                # part already determined. no tiebreak_info is needed
                self.increment_counter(
                    "v%s MAP INFO" % fhr_version, "unlinkable; no datePrints")
                yield "|".join(["unlinkable", fhr_version, docid]), "u"
                return
        else:
            # if there is no tiebreak_info, the packet is bad.
            return

    def reducer(self, composite_key, vals):
        kv_type, fhr_version, key = composite_key.split("|")

        # pass tiebreak_infocurrentSessionTimecurrentSessionTime /c/ and unlinkable k/v pairs straight through
        # k/v pairs recieved for "unlinkable" and "kDocId_vTieBreakInfo" should
        # be unique EXCEPT in the case of identical docids and exactly
        # duplicated records. in these cases, it suffices to re-emit the first
        # element of each list
        if kv_type == "unlinkable" or kv_type == "kDocId_vTieBreakInfo":
            self.increment_counter(
                "v%s REDUCER" % fhr_version, "%s passed through reducer" % kv_type)
            yield "v%s/%s|%s" % (fhr_version, kv_type, key), next(vals)

        elif kv_type == 'kDatePrint_vDocId':
            self.increment_counter(
                "v%s REDUCER" % fhr_version, "datePrint key into reducer")
            # in this case, we have a datePrint key. this reducer does all the
            # datePrint linkage and initializes the parts; choose the lowest
            # corresponding docid to initialize the part. subsequent steps will
            # link parts through docs, and relabel docs into the lowest
            # connected part
            linked_doc_ids = list(vals)
            part_num = min(linked_doc_ids)
            for docid in linked_doc_ids:
                yield "v%s/kDoc_vPart_0|%s" % (fhr_version, docid),  "p" + part_num
                self.increment_counter(
                    "v%s REDUCER" % fhr_version, "kDoc_vPart_0 out from reducer")

        else:
            self.increment_counter("REDUCER ERROR", "bad key type")


if __name__ == '__main__':
    ScanJob.run()
