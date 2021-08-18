from constraints.enums.constraint_input_mode import ConstraintInputMode
from constraints.enums.model_family import ModelFamily
from constraints.constraint_main.constraint import Constraint
from constraints.enums.input_type import InputType
from constraints.models.model_parent import Model
import datetime


class TimeRangeModel(Model):
    def __init__(self):
        self.name = "TimeRangeModel"
        self.model_family = ModelFamily.CONSTRAINT
        self.input_type = InputType.ANY
        self.input_mode = ConstraintInputMode.PRE_DEF
        self.input_count = 0
        self.output_type = InputType.BOOL
        self.config_parameters = ["start_hour", "start_min",
                                  "end_hour", "end_min", "start_day", "end_day"]

        super().__init__(self.name, self.model_family, self.input_type,
                         self.input_mode, self.input_count, self.output_type, configuration_input_required=True, configuration_input_count=6, config_parameters=self.config_parameters)

    def run(self, inputs, configuration_inputs={}):
        super().run(inputs)
        now = datetime.datetime.now()

        start_hour = self._get_configuration_input_value("start_hour")
        start_min = self._get_configuration_input_value("start_min")
        end_hour = self._get_configuration_input_value("end_hour")
        end_min = self._get_configuration_input_value("end_min")
        start_day = self.day_to_num(
            self._get_configuration_input_value("start_day"))
        end_day = self.day_to_num(
            self._get_configuration_input_value("end_day"))

        print(self.constraint.configuration_inputs)
        print(now.hour)
        print(now.minute)
        print(now.weekday())
        print("start_hour", start_hour)
        print("start_min", start_min)
        print("end_hour", end_hour)
        print("end_min", end_min)
        print("start_day", start_day)
        print("end_day", end_day)

        while True:
            if self.isNowInTimePeriod(datetime.time(start_hour, start_min), datetime.time(end_hour, end_min), now.time()) and start_day <= now.weekday() <= end_day:
                self._complete(True)
                print("done")
                break

    def day_to_num(self, day: str):
        if (day == "Monday"):
            return 0
        elif (day == "Tuesday"):
            return 1
        elif (day == "Wednesday"):
            return 2
        elif (day == "Thursday"):
            return 3
        elif (day == "Friday"):
            return 4
        elif (day == "Saturday"):
            return 5
        elif (day == "Sunday"):
            return 6

    def isNowInTimePeriod(self, startTime, endTime, nowTime):
        if startTime < endTime:
            return nowTime >= startTime and nowTime <= endTime
        else:
            # Over midnight:
            return nowTime >= startTime or nowTime <= endTime

    def _complete(self, data, aborted=False):
        super()._complete(data, aborted)
