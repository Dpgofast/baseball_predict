class clean:
    """
    """

    def __init__(self):
        pass

    self.drop_columns = [
        'spin_dir',
        'spin_rate_deprecated',
        'break_angle_deprecated',
        'break_length_deprecated',
        'game_type',
        'tfs_deprecated',
        'tfs_zulu_deprecated',
        'umpire'
        ]

    def drop_columns(columns=self.drop_columns):
        """
        Drop depreciated and extraneous features
        """
        df = df.drop(columns)

    def sort_pitches():
        """
        Sort pitches chronologically
        """
        df = df.sort_values(by = [
            'game_date',
            'game_pk',
            'at_bat_number',
            'pitch_number'
            ])

    def pitch_types_cleaner():
        """
        Fills pitch type nan with a random choice with the pitch type
        prior probability as weights
        """
        # Replace UN with nan
        df['pitch_type'] = df['pitch_type'].replace({'UN':np.nan})

        # Get all the unique pitcher names in the df
        pitcher_list = df.player_name.unique().tolist()

        # Initialize empty dictionary to store each pitcher and their pitches and
        # Percentages for each pitch
        pitcher_dict = {}

        # Iterate over each pitcher:
        for pitcher in pitcher_list:
            # Assign the normalized value_counts to a variable
            pitch_percentages = df[df.player_name == pitcher].pitch_type.value_counts(normalize=True)
            # Convert that Series object to a dict and assign it as the value to
            # The pitcher dictionary
            # (Pitcher name as key)
            pitcher_dict[pitcher] = pitch_percentages.to_dict()

        # Grab the rows where pitch_type is null:
        nulls = df[df.pitch_type.isna()]

        # Iterate over each null row and assign value
        for index, row in nulls.iterrows():
          population = list(pitcher_dict[row.player_name].keys())
          weights = list(pitcher_dict[row.player_name].values())
          try:
            pitch = random.choices(population, weights, k=1)[0]
          except IndexError:
            pitch = 'FF'
          df.at[index, 'pitch_type'] = pitch


    def create_pitch_category():
        """
        """
        # Create map for pitch type into categories:
        pitch_type_map = {
            'FA':'fastball', 'FF':'fastball', 'FT':'fastball', 'FC':'fastball',
            'FS':'fastball', 'SI':'fastball', 'SF':'fastball', 'SL':'breaking',
            'CB':'breaking', 'CU':'breaking', 'SC':'breaking', 'KC':'breaking',
            'CH':'offspeed', 'KN':'offspeed', 'EP':'offspeed', 'FO':'pitchout',
            'PO':'pitchout'
            }

        # Create pitch cateogory feature
        df['pitch_cat'] = df['pitch_type']
        df['pitch_cat'] = df['pitch_cat'].replace(pitch_type_map)


    def typecast_columns():
        df['if_fielding_alignment'] = df['if_fielding_alignment'].astype(object)
        df['of_fielding_alignment'] = df['of_fielding_alignment'].astype(object)


    def create_swung():
        """
        Create swung feature
        """
        def swung(x):
            swung = [
                'foul','hit_into_play','swinging_strike','hit_into_play_no_out',
                'hit_into_play_score','foul_tip','swinging_strike_blocked',
                'foul_bunt','missed_bunt'
            ]
            return 1 if x in swung else 0
        df['batter_swung'] = df['description'].apply(swung)


    def create_strikezone_swung_chase():
        """
        """
        # Initialize in_strikezone and chased features:
        df['in_strikezone'] = 1
        df['chased'] = 0

        # Iterate through each row
        for index, row in df.iterrows():
            #if ball is outside the strikezone, change the value for that row to 0
            if row.plate_z > row.sz_top or row.plate_z < row.sz_bot or row.plate_x < -0.73 or row.plate_x > 0.73:
                df.at[index, 'in_strikezone'] = 0
            #if batter_swung at ball outside the strike zone, change chased value to 1:
            if df.at[index, 'batter_swung'] == 1 and df.at[index, 'in_strikezone'] == 0:
                df.at[index, 'chased'] = 1


    def run(df):
        """
        Clean
        """
        df = df.copy()
        drop_columns()
        sort_pitches()
        pitch_types_cleaner()
        create_pitch_category()
        typecast_columns()
        create_swung()
        create_strikezone_swung_chase()
        return df