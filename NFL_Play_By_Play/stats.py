from metaflow import FlowSpec, step, IncludeFile
import pandas
import numpy as np

class NFLStatsFlow(FlowSpec):
    """
    A flow to collect some information about the Baltimore Ravens plays in 2019.

    The flow performs the following steps:
    1) Ingests a CSV into a Pandas Dataframe.
    2) Filter play by play data.
    3) Add drive-level metrics.

    """
    nfl_data = IncludeFile("nfl_data",
                             help="The path to a nfl play by play metadata file.",
                             default=('reg_pbp_2019.csv'))
    @step
    def start(self):
        """
        The start step:
        1) Loads the data into pandas dataframe.
        2) Adds full play type (pass right, left, center; run left, right, center).
        3) Adds rushing/passing specific metrics.

        """
        from io import StringIO

        # Load the data set into a pandas dataaframe.
        self.nfl_dataframe = pandas.read_csv(StringIO(self.nfl_data))

        # Filter data down to Baltimore Ravens
        self.baltimore_df = self.nfl_dataframe[ \
            (self.nfl_dataframe.posteam=='BAL') & \
            (self.nfl_dataframe.down.isin(range(1,5))) & \
            ((self.nfl_dataframe.play_type=='run') | (self.nfl_dataframe.play_type == 'pass')) & \
            (self.nfl_dataframe.qb_spike==0) & \
            (self.nfl_dataframe.qb_kneel==0)
        ]
        def get_full_play_type(play):
            play_type, pass_location, pass_length, run_location = play
            if(play_type == 'run'):
                return play_type+'_'+ run_location
            else:
                return play_type+'_'+ pass_location+'_'+pass_length

        self.baltimore_df = self.baltimore_df.replace(np.nan, 'unknown', regex=True)
        self.baltimore_df['full_play_type'] = self.baltimore_df[['play_type','pass_location', 'pass_length','run_location']].apply(get_full_play_type, axis=1)
        self.baltimore_df = self.baltimore_df[(self.baltimore_df.full_play_type.isin(['pass_left_short','pass_left_long', 'pass_middle_short','pass_middle_long', 'pass_right_short','pass_right_long','run_left', 'run_middle', 'run_right']))]


        self.baltimore_df['rushing_yards_gained'] = np.where(self.baltimore_df['play_type']=='run', self.baltimore_df['yards_gained'], 0)
        self.baltimore_df['passing_yards_gained'] = np.where(self.baltimore_df['play_type']=='pass', self.baltimore_df['yards_gained'], 0)

        self.next(self.drive_level_index)

    @step
    def drive_level_index(self):
        """
        Create an unique identifier for a drive + game id
        """
        self.baltimore_df['unique_drive'] = self.baltimore_df.apply(lambda row: str(row['game_id']) + '_' + str(row['drive']), axis=1)
        self.next(self.drive_level_metrics)

    @step
    def drive_level_metrics(self):
        """
        Adds metrics about the specific drive (ie rushing yards, penalties, sacks, etc)
        """

        def get_cumulative_data(metric, new_name, fill_blanks = 0):
            a = self.baltimore_df.groupby('unique_drive')[metric].cumsum()
            a.name = new_name + '_after'
            self.baltimore_df = pandas.concat([self.baltimore_df, a], axis = 1)
            self.baltimore_df[new_name] = self.baltimore_df.groupby(['unique_drive'])[a.name].shift(1)
            self.baltimore_df[new_name].fillna(fill_blanks, inplace=True)

        def get_rank_data(metric, new_name, fill_blanks = 0):
            a = self.baltimore_df.groupby('unique_drive')[metric].rank(ascending = True, method = 'first')
            a.name = new_name
            self.baltimore_df = pandas.concat([self.baltimore_df, a], axis = 1)

        metric_list = [
            {
                "new_name": "drive_yards_gained",
                "metric": "yards_gained",
                "type": 'cumulative_data',
                "fill_blanks": 0
            },
            {
                "new_name": "rushing_drive_yards_gained",
                "metric": "rushing_yards_gained",
                "type": 'cumulative_data',
                "fill_blanks": 0
            },
            {
                "new_name": "passing_drive_yards_gained",
                "metric": "passing_yards_gained",
                "type": 'cumulative_data',
                "fill_blanks": 0
            },
            {
                "new_name": "drive_sacks",
                "metric": "sack",
                "type": 'cumulative_data',
                "fill_blanks": 0
            },
            {
                "new_name": "drive_incomplete_pass",
                "metric": "incomplete_pass",
                "type": 'cumulative_data',
                "fill_blanks": 0
            },
            {
                "new_name": "drive_no_huddles",
                "metric": "no_huddle",
                "type": 'cumulative_data',
                "fill_blanks": 0
            },
            {
                "new_name": "drive_play_count",
                "metric": "play_id",
                "type": 'rank',
                "fill_blanks": 0
            },
        ]

        for i in metric_list:
            if(i['type'] == 'cumulative_data'):
                get_cumulative_data(
                    metric = i['metric'],
                    new_name = i['new_name'],
                    fill_blanks = i['fill_blanks']
                )
            else:
                get_rank_data('play_id','drive_play_count',0)

        self.baltimore_df['previous_play_in_drive'] = self.baltimore_df.groupby(['unique_drive'])['full_play_type'].shift(1)
        self.baltimore_df['previous_play_in_drive'].fillna('first_play', inplace=True)

        # self.baltimore_df.drop(columns=['drive_yards_gained_after', 'passing_drive_yards_gained_after', 'rushing_drive_yards_gained_after'])
        self.next(self.end)
    @step
    def end(self):
        """
        End the flow.

        """
        pass

if __name__ == '__main__':
    NFLStatsFlow()
