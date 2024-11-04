class BaseTimer():

    def get_next_time(self,) -> int:
        """
            return time interval after which some states will change
        """
        pass

    def step(self, time_step):
        """
            change states accoring to the time step
        """
        pass

