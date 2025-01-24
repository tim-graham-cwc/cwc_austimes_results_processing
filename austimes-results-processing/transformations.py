### UNDER DEV

import pandas as pd

class Transform:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def gap_fill(self):
        # List of years for which the dataframe has data
        dataframe_columns = self.dataframe.columns.values.tolist()
        data_years = []
        for i in dataframe_columns:
            try:
                x = int(i)
                data_years.append(x)
            except ValueError:
                pass
        print(data_years)
        # List for all years between first and last year
        all_years = [x for x in range(data_years[0], data_years[-1] + 1)]
        print(all_years)

        interp_dict = {
            "year": [],
            "years_since": [],
            "years_to": []
        }

        n = 0
        for year in all_years:
            interp_dict['year'].append(year)
            if year in data_years:
                n = 0
                interp_dict['years_since'].append(n)
            else:
                n += 1
                interp_dict['years_since'].append(n)
        m = 0
        for year in reversed(all_years):
            if year in data_years:
                m = 0
                interp_dict['years_to'].insert(0, m)
            else:
                m += 1
                interp_dict['years_to'].insert(0, m)
            # Interpolating data in emissions summary sheet
        for index, row in self.dataframe.iterrows():
            n = 0
            for y in interp_dict['year']:
                ys = interp_dict['years_since'][n]
                yt = interp_dict['years_to'][n]
                # print("Years since: " + str(ys))
                # print("Years to: " + str(yt))
                current_year = str(y)
                next_year = str(y + yt)
                previous_year = str(y - ys)
                # print("current year: " + str(current_year))
                # print("next year: " + str(next_year))
                # print("previous year: " + str(previous_year))
                if ys != 0:
                    step = (self.dataframe.at[index, next_year] - self.dataframe.at[index, previous_year]) / (ys + yt)
                    self.dataframe.at[index, current_year] = self.dataframe.at[index, previous_year] + (step * ys)
                    # dataframe.at[index, current_year] = interp_value
                else:
                    pass
                n += 1
        self.dataframe = self.dataframe[[str(x) for x in all_years]]
        return self.dataframe

df = pd.read_csv("example.csv")

transform = Transform(df)

df_trans = transform.gap_fill()

print(df_trans)