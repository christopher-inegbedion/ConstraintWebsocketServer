from constraints.enums.constraint_input_mode import ConstraintInputMode
from constraints.enums.model_family import ModelFamily
from constraints.enums.input_type import InputType
from constraints.models.model_parent import Model


class OrderProductModel(Model):
    def __init__(self):
        self.name = "OrderProductModel"
        self.model_family = ModelFamily.CONSTRAINT
        self.input_type = InputType.BOOL
        self.input_mode = ConstraintInputMode.PRE_DEF
        self.input_count = 1
        self.output_type = InputType.BOOL

        super().__init__(self.name, self.model_family, self.input_type,
                         self.input_mode, self.input_count, self.output_type)

    def run(self, inputs, configuration_inputs={}):
        super().run(inputs)

        self._complete(True)

    def _complete(self, data, aborted=False):
        super()._complete(data, aborted)
