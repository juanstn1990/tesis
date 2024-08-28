import datetime


import datetime

def create_trimesters(initial_date, last_date):
    initial_date = datetime.datetime.strptime(initial_date, "%Y-%m-%d")
    last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d")
    trimesters = []
    
    while initial_date < last_date:
        trimester_start = initial_date.strftime("%Y-%m-%d")
        trimester_end = (initial_date + datetime.timedelta(days=7)).strftime("%Y-%m-%d")  # 90 days - 1 day for the end of the trimester
        
        # Append the trimester with the adjusted time
        trimesters.append({
            "trimester_start": trimester_start + "T00:00:00.000",
            "trimester_end": trimester_end + "T23:59:59.000"
        })
        
        # Move to the start of the next trimester (add 90 days)
        initial_date = initial_date + datetime.timedelta(days=8)
    
    return trimesters
