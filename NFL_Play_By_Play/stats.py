from metaflow import FlowSpec, step, IncludeFile
import pandas
import numpy as np

class NFLStatsFlow(FlowSpec):
    """
    A flow to collect some information about the Baltimore Ravens plays in 2019.

    The flow performs the following steps:
    1) Ingests a CSV into a Pandas Dataframe.
    2) Filter play by play data.
    3) Add additional metrics at the drive and game level.

    """
    nfl_data = IncludeFile("nfl_data",
            help="The path to a nfl play by play metadata file.",
            default=('reg_pbp_2018.csv')
        )
    @step
    def start(self):
        """
        The start step:
            1) Loads the data into pandas dataframe and filter based on relevant
            2) Add full play type.
            3) Add rushing/passing specific metrics.
        """
        from io import StringIO
        ########################################################
        ###1) Loads the data into pandas dataframe and filter ##
        ########################################################
        self.nfl_dataframe = pandas.read_csv(StringIO(self.nfl_data))

        # Filter data down to Baltimore Ravens
        self.baltimore_df = self.nfl_dataframe[ \
            (self.nfl_dataframe.posteam=='BAL') & \
            (self.nfl_dataframe.down.isin(range(1,5))) & \
            ((self.nfl_dataframe.play_type=='run') | (self.nfl_dataframe.play_type == 'pass')) & \
            (self.nfl_dataframe.qb_spike==0) & \
            (self.nfl_dataframe.qb_kneel==0)
        ]

        ########################################################
        ################ 2) Add full play type #################
        ########################################################

        from modules import get_full_play_type

        self.baltimore_df = self.baltimore_df.replace(np.nan, 'unknown', regex=True)
        self.baltimore_df['full_play_type'] = self.baltimore_df[['play_type','pass_location', 'pass_length','run_location']].apply(get_full_play_type, axis=1)
        self.baltimore_df = self.baltimore_df[(self.baltimore_df.full_play_type.isin(['pass_left_short','pass_left_deep', 'pass_middle_short','pass_middle_deep', 'pass_right_short','pass_right_deep','run_left', 'run_middle', 'run_right']))]

        ########################################################
        ####### 3) Add rushing/passing specific metrics. #######
        ########################################################

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

        def get_cumulative_data(dataframe, metric, new_name, fill_blanks = 0, granularity = 'drive'):
            index_name = 'unique_drive'
            if (granularity == 'game'):
                index_name = 'game_id'
            a = dataframe.groupby(index_name)[metric].cumsum()
            a.name = new_name + '_after'
            dataframe = pandas.concat([dataframe, a], axis = 1)
            dataframe[new_name] = dataframe.groupby([index_name])[a.name].shift(1)
            dataframe[new_name].fillna(fill_blanks, inplace=True)

        def get_rank_data(metric, new_name, fill_blanks = 0, granularity = 'drive'):
            index_name = 'unique_drive'
            if (granularity == 'game'):
                index_name = 'game_id'
            a = self.baltimore_df.groupby(index_name)[metric].rank(ascending = True, method = 'first')
            a.name = new_name
            self.baltimore_df = pandas.concat([self.baltimore_df, a], axis = 1)

        from metric_list import metric_list

        for i in metric_list:
            if(i['type'] == 'cumulative_data'):
                get_cumulative_data(
                    metric = i['metric'],
                    new_name = 'drive'+'_'+i['metric'],
                    fill_blanks = i['fill_blanks'],
                    granularity = 'drive'
                )
                get_cumulative_data(
                    metric = i['metric'],
                    new_name = 'game'+'_'+i['metric'],
                    fill_blanks = i['fill_blanks'],
                    granularity = 'game'
                )
            else:
                get_rank_data(
                    metric = i['metric'],
                    new_name = 'game'+'_'+i['new_name'],
                    fill_blanks = i['fill_blanks'],
                    granularity = 'game'
                )

        self.baltimore_df['previous_play_in_drive'] = self.baltimore_df.groupby(['unique_drive'])['full_play_type'].shift(1)
        self.baltimore_df['previous_play_in_drive'].fillna('first_play', inplace=True)

        self.next(self.end)
    @step
    def end(self):
        """
        End the flow.

        """
        pass

if __name__ == '__main__':
    NFLStatsFlow()
