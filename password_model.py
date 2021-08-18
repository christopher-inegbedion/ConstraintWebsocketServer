from constraints.enums.constraint_input_mode import ConstraintInputMode
from constraints.enums.model_family import ModelFamily
from constraints.constraint_main.constraint import Constraint
from constraints.enums.input_type import InputType
from constraints.models.model_parent import Model
import requests
import jsonpickle


class PasswordModel(Model):
    def __init__(self):
        self.name = "PasswordModel"
        self.model_family = ModelFamily.CONSTRAINT
        self.input_type = InputType.STRING
        self.input_mode = ConstraintInputMode.PRE_DEF
        self.input_count = 1
        self.output_type = InputType.BOOL
        self.config_parameters = [
            "passcode"]

        super().__init__(self.name, self.model_family, self.input_type,
                         self.input_mode, self.input_count, self.output_type, configuration_input_required=True, configuration_input_count=1, config_parameters=self.config_parameters)

    def run(self, inputs, configuration_inputs={}):
        super().run(inputs)

        value = inputs[0]
        constraint: Constraint = self.constraint
        if value == constraint.configuration_inputs["passcode"]:
            self.external_action(
                False, self.constraint.name, "com", {"data": "test"})
            self._complete(True)

    def _complete(self, data, aborted=False):
        super()._complete(data, aborted)
