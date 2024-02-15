import logging
from azure import functions as func
from musicdata import MusicData

app = func.FunctionApp()

@app.schedule(schedule="0 0 10 * * Mon", arg_name="myTimer", run_on_startup=False, use_monitor=True) 
def dailymusic_slack(myTimer: func.TimerRequest) -> None:
    print(myTimer)
    music = MusicData.blob_to_df('daily_music.csv')
    music.select_theme()
    music.upload_csv()
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
