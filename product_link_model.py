from constraints.enums.constraint_input_mode import ConstraintInputMode
from constraints.enums.model_family import ModelFamily
from constraints.enums.input_type import InputType
from constraints.models.model_parent import Model


class ProductLinkModel(Model):
    def __init__(self):
        self.name = "ProductLinkModel"
        self.model_family = ModelFamily.CONSTRAINT
        self.input_type = InputType.BOOL
        self.input_mode = ConstraintInputMode.PRE_DEF
        self.input_count = 1
        self.output_type = InputType.BOOL
        self.config_parameters = [
            "Link header", "Link description", "Link btn name", "Link url"]

        super().__init__(self.name, self.model_family, self.input_type,
                         self.input_mode, self.input_count, self.output_type, configuration_input_required=True, configuration_input_count=4, config_parameters=self.config_parameters)

    def run(self, inputs, configuration_inputs={}):
        super().run(inputs) 
        print("done")

        self._complete(True)

    def _complete(self, data, aborted=False):
        super()._complete(data, aborted)
