from pyflink.datastream import ProcessWindowFunction


class ReachProcess(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        users = {user_id for (_, _, user_id) in elements}
        out.collect({"ad_id": key, "reach": len(users)})