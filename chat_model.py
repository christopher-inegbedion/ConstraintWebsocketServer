from time import sleep
from constraints.constraint_main.constraint import Constraint
from constraints.enums.constraint_input_mode import ConstraintInputMode
from constraints.enums.input_type import InputType
from constraints.enums.model_family import ModelFamily
from constraints.models.model_parent import Model


class ChatModel(Model):
    def __init__(self):
        self.name = "ChatModel"
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
        if msg == "user":
            self._set_configuration_input_value("sender", "user")
            self._notify_config_input_change()
        elif msg == "admin":
            self._set_configuration_input_value("sender", "admin")
            self._notify_config_input_change()
        elif msg == "complete":
            self._set_configuration_input_value("sender", "complete")
            self._notify_config_input_change()
            self._complete(False)
            
        print(self._get_configuration_input_value("msg"))

    def _complete(self, data, aborted=False):
        super()._complete(data, aborted)
