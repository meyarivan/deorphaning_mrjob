from mrjob.job import MRJob
import mrjob
import sys
import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)


# inputs to this job will be:
# full records: (docId,fhrPayload)
# unlinkable docids: (docId,"u")
# head record docIds: (docId,"h")


class FinalRecordExtractionJob(MRJob):
    HADOOP_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat"
    INPUT_PROTOCOL = mrjob.protocol.RawProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def mapper(self, docid, val):
        self.increment_counter("MAPPER", "input k/v pair")
        yield(docid, val)

    def reducer(self, docid, vals):
        # not all docids will be re-emitted; only emit jsons when there is a
        # flag of either "u" or "h"
        emit_json = False
        out_value = None

        for v in vals:
            if v[0] in ["u", "h"]:
                emit_json = True
            elif v[0] == "{":
                out_value = v
            else:
                raise ValueError()

        if emit_json:
            self.increment_counter("REDUCER", "final record out")
            yield(docid, out_value)
        else:
            self.increment_counter("REDUCER", "final records dropped")


if __name__ == '__main__':
    FinalRecordExtractionJob.run()
