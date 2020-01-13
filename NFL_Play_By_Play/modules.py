
import pandas

def get_full_play_type(play):
    play_type, pass_location, pass_length, run_location = play
    if(play_type == 'run'):
        return play_type+'_'+ run_location
    else:
        return play_type+'_'+ pass_location+'_'+pass_length

# def get_cumulative_data(dataframe, metric, new_name, fill_blanks = 0, granularity = 'drive'):
#     index_name = 'unique_drive'
#     if (granularity == 'game'):
#         index_name = 'game_id'
#     a = dataframe.groupby(index_name)[metric].cumsum()
#     a.name = new_name + '_after'
#     dataframe = pandas.concat([dataframe, a], axis = 1)
#     dataframe[new_name] = dataframe.groupby([index_name])[a.name].shift(1)
#     dataframe[new_name].fillna(fill_blanks, inplace=True)
#
# def get_rank_data(dataframe, metric, new_name, fill_blanks = 0, granularity = 'drive'):
#     index_name = 'unique_drive'
#     if (granularity == 'game'):
#         index_name = 'game_id'
#     a = dataframe.groupby(index_name)[metric].rank(ascending = True, method = 'first')
#     a.name = new_name
#     dataframe = pandas.concat([dataframe, a], axis = 1)
