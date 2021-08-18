from time import sleep
from constraints.constraint_main.constraint import Constraint
from constraints.enums.constraint_input_mode import ConstraintInputMode
from constraints.enums.input_type import InputType
from constraints.enums.model_family import ModelFamily
from constraints.models.model_parent import Model


class DeliveryModel(Model):
    def __init__(self):
        self.name = "DeliveryModel"
        self.model_family = ModelFamily.CONSTRAINT
        self.input_type = InputType.STRING
        self.input_mode = ConstraintInputMode.PRE_DEF
        self.input_count = 0
        self.output_type = InputType.ANY

        super().__init__(self.name, self.model_family, self.input_type,
                         self.input_mode, self.input_count, self.output_type, admin_session_independent=False)

    def listen(self, msg, data):
        self._set_configuration_input_value(
            "msg", data)
        if msg == "start":
            self._set_configuration_input_value("stage", "start")
            self._notify_config_input_change()
        elif msg == "prep":
            self._set_configuration_input_value("stage", "prep")
            self._notify_config_input_change()
        elif msg == "en_route":
            self._set_configuration_input_value("stage", "en_route")
            self._notify_config_input_change()
        elif msg == "done":
            self._set_configuration_input_value("stage", "done")
            self._notify_config_input_change()
            self._complete(True)

    def _complete(self, data, aborted=False):
        super()._complete(data, aborted)
