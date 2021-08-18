from constraints.enums.constraint_input_mode import ConstraintInputMode
from constraints.enums.model_family import ModelFamily
from constraints.enums.input_type import InputType
from constraints.models.model_parent import Model
import requests
import jsonpickle


class ProductDescriptionModel(Model):
    def __init__(self):
        self.name = "ProductDesctiptionModel"
        self.model_family = ModelFamily.CONSTRAINT
        self.input_type = InputType.STRING
        self.input_mode = ConstraintInputMode.PRE_DEF
        self.input_count = 0
        self.output_type = InputType.ANY
        self.config_parameters = ["Product name", "Product description"]

        super().__init__(self.name, self.model_family, self.input_type,
                         self.input_mode, self.input_count, self.output_type, configuration_input_required=True, configuration_input_count=2, config_parameters=self.config_parameters)

    def run(self, inputs, configuration_inputs={}):
        super().run(inputs)

        print("done done")
        self._complete(True)

    def _complete(self, data, aborted=False):
        super()._complete(data, aborted)
