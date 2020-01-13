from metaflow import FlowSpec, step, IncludeFile
import pandas
import numpy as np
def script_path(filename):
    """
    A convenience function to get the absolute path to a file in this
    tutorial's directory. This allows the tutorial to be launched from any
    directory.
    """
    import os

    filepath = os.path.join(os.path.dirname(__file__))
    return os.path.join(filepath, filename)


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
                             default=script_path('reg_pbp_2019.csv'))
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
            ((self.nfl_dataframe.play_type=='run') | (self.nfl_dataframe.play_type == 'pass')) \
        ]
        def get_full_play_type(play):
            play_type, pass_location, run_location = play
            if(play_type == 'run'):
                return play_type+'_'+ run_location
            else:
                return play_type+'_'+ pass_location

        self.baltimore_df = self.baltimore_df.replace(np.nan, 'unknown', regex=True)
        self.baltimore_df['full_play_type'] = self.baltimore_df[['play_type','pass_location', 'run_location']].apply(get_full_play_type, axis=1)
        self.baltimore_df = self.baltimore_df[(self.baltimore_df.full_play_type.isin(['pass_left', 'pass_middle','pass_right','run_left', 'run_middle', 'run_right']))]

        self.baltimore_df = self.baltimore_df[~self.baltimore_df['desc'].str.contains("kneels")]
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
        # Drive Yards
        drive_play_count = self.baltimore_df \
            .groupby('unique_drive')['play_id'] \
            .rank(ascending = True, method = 'first')
        drive_play_count.name = 'drive_play_count'
        self.baltimore_df = pandas.concat([self.baltimore_df, drive_play_count], axis = 1)

        drive_yards = self.baltimore_df \
            .groupby('unique_drive')['yards_gained'] \
            .cumsum()
        drive_yards.name = 'drive_yards_gained_after'

        rushing_drive_yards = self.baltimore_df \
            .groupby('unique_drive')['rushing_yards_gained'] \
            .cumsum()
        rushing_drive_yards.name = 'rushing_drive_yards_gained_after'

        passing_drive_yards = self.baltimore_df \
            .groupby('unique_drive')['passing_yards_gained'] \
            .cumsum()
        passing_drive_yards.name = 'passing_drive_yards_gained_after'

        self.baltimore_df = pandas.concat([self.baltimore_df, drive_yards], axis = 1)
        self.baltimore_df = pandas.concat([self.baltimore_df, rushing_drive_yards], axis = 1)
        self.baltimore_df = pandas.concat([self.baltimore_df, passing_drive_yards], axis = 1)

        self.baltimore_df['drive_yards_gained'] = self.baltimore_df.groupby(['unique_drive'])['drive_yards_gained_after'].shift(1)
        self.baltimore_df['drive_yards_gained'].fillna(0, inplace=True)

        self.baltimore_df['drive_rushing_yards_gained'] = self.baltimore_df.groupby(['unique_drive'])['rushing_drive_yards_gained_after'].shift(1)
        self.baltimore_df['drive_rushing_yards_gained'].fillna(0, inplace=True)

        self.baltimore_df['drive_passing_yards_gained'] = self.baltimore_df.groupby(['unique_drive'])['passing_drive_yards_gained_after'].shift(1)
        self.baltimore_df['drive_passing_yards_gained'].fillna(0, inplace=True)

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
