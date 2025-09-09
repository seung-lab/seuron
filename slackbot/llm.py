import json
import urllib
import requests
from enum import StrEnum, auto
from typing import Literal, TypedDict 

from airflow.models import Variable

from pydantic import BaseModel, Field, ValidationError
from langchain_google_vertexai import ChatVertexAI
from langchain_core.tools import tool
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.chat_history import InMemoryChatMessageHistory

from cloudfiles import dl

from llm_types import RawSegParams, ValidatedSegParams, InferredParams
from bot_info import botid, workerid
from bot_utils import replyto


class IntentEnum(StrEnum):
    create_segmentation = auto()
    downsample = auto()
    transfer = auto()
    follow_up = auto()
    irrelevant = auto()

    def describe(self) -> str:
        return {
            "create_segmentation": "New conversation to create segmentation of a cutout from image stack or affinity map",
            "downsample": "New conversation to downsample a dataset",
            "transfer": "New conversation to transfer a dataset",
            "follow_up": "The intention does not change, user supply additional input parameters for the current conversation",
            "irrelevant": "The message is irrelevant of current conversation and does not fit any other intentions",
        }[self.value]


intent_description = "\n".join(
    f"{item.value}: {item.describe()}" for item in IntentEnum
)


class UserIntent(BaseModel):
    intent: IntentEnum = Field(..., description=f"The user's intention. Options:\n{intent_description}")


extraction_prompt = ChatPromptTemplate.from_messages([
    ("system", """
You are an extraction assistant. Your task is to read the chat log and extract information needed for segmentation or inference and output a json dictionary containing those informations, you can use the variables already collected in the previous conversation, but if user specified a new value overwrite the existing variables.
"""),
    MessagesPlaceholder(variable_name="chat_history"),
    ("human", """
User input: {input},
layers extracted from neuroglancer link: {ng_layers},
inferred parameters: {inferred_params},
knowledge: {knowledge}
""")
])

exception_prompt = ChatPromptTemplate.from_messages([
    ("system", """Confirm the input parameters from the user that are not null, then process the error messages from input data validation, explain the errors to user with suggestions. List the value of the fields explicitly if not null, But do not mention the name of the field directly, use the description of the value. Make the message sounds like Gordon Ramsay"""),
    MessagesPlaceholder("chat_history"),
    ("human", "Input parammeters: {input}, layers extracted from neuroglancer link: {ng_layers}, inferred parameters: {inferred_params}, validation errors: {errors}")
])

classifier_prompt = ChatPromptTemplate.from_messages([
    ("system", """Determine the users intention based on the user input and chat history"""),
    MessagesPlaceholder("chat_history"),
    ("human", "User said: {input}")
])

nglink_prompt = ChatPromptTemplate.from_messages([
    ("system", """Extract neuroglancer payload in the user input message, use the function provided, the neuroglancer links starts with https://spelunker.cave-explorer.org or https://neuroglancer-demo.appspot.com, the neuroglancer json payload may have 3 forms:
1. Referred to a json file stored in google cloud storage:
   Example:
   https://neuroglancer-demo.appspot.com/fafb.html#!gs://fafb-ffn1/main_ng.json
2. Referred in a nglstate link:
   Example:
   https://spelunker.cave-explorer.org/#!middleauth+https://global.daf-apis.com/nglstate/api/v1/5707813870370816
3. Direct encoded in the url as escaped strings, ONLY fallback to this if the neuroglancer link does not match the first two options.
   Example:
   https://spelunker.cave-explorer.org/#!%7B%22layers%22%3A%20%7B%22img%22%3A%20%7B%22source%22%3A%20%22precomputed%3A//gs%3A//neuroglancer-janelia-flyem-hemibrain/emdata/clahe_yz/jpeg%22%2C%20%22type%22%3A%20%22image%22%7D%2C%20%22aff%22%3A%20%7B%22source%22%3A%20%22precomputed%3A//gs%3A//ng_scratch_ranl/llm/20250724222949/inference%22%2C%20%22shader%22%3A%20%22%23uicontrol%20invlerp%20normalized%5Cnvoid%20main%28%29%20%7B%5Cn%20%20float%20r%20%3D%20toNormalized%28getDataValue%280%29%29%3B%5Cn%20%20float%20g%20%3D%20toNormalized%28getDataValue%281%29%29%3B%5Cn%20%20float%20b%20%3D%20toNormalized%28getDataValue%282%29%29%3B%20%5Cn%20%20emitRGB%28vec3%28r%2Cg%2Cb%29%29%3B%5Cn%7D%22%2C%20%22type%22%3A%20%22image%22%2C%20%22visible%22%3A%20false%7D%2C%20%22ws%22%3A%20%7B%22source%22%3A%20%22precomputed%3A//gs%3A//ng_scratch_ranl/llm/20250724222949/ng/ws/20250724224918%22%2C%20%22type%22%3A%20%22segmentation%22%2C%20%22visible%22%3A%20false%7D%2C%20%22seg%22%3A%20%7B%22source%22%3A%20%22precomputed%3A//gs%3A//ng_scratch_ranl/llm/20250724222949/ng/seg/20250724224918%22%2C%20%22type%22%3A%20%22segmentation%22%2C%20%22segments%22%3A%20%5B%2272057662757508789%22%2C%20%2272127962782244374%22%2C%20%2272057594038014886%22%2C%20%2272057594038117890%22%2C%20%2272057594038115996%22%2C%20%2272128031501642503%22%2C%20%2272057662757594098%22%2C%20%2272057662757660912%22%2C%20%2272057662757481572%22%2C%20%2272128031501801653%22%2C%20%2272057594038182258%22%2C%20%2272057594038122240%22%2C%20%2272128031501733966%22%2C%20%2272128031501664824%22%2C%20%2272128031501798852%22%2C%20%2272057662757536237%22%2C%20%2272127962782108323%22%2C%20%2272128031501774792%22%2C%20%2272127962782146777%22%2C%20%2272127962782194939%22%5D%7D%2C%20%22size%22%3A%20%7B%22source%22%3A%20%22precomputed%3A//gs%3A//ng_scratch_ranl/llm/20250724222949/ng/seg/20250724224918/size_map%22%2C%20%22type%22%3A%20%22image%22%2C%20%22visible%22%3A%20false%7D%7D%2C%20%22navigation%22%3A%20%7B%22pose%22%3A%20%7B%22position%22%3A%20%7B%22voxelSize%22%3A%20%5B8%2C%208%2C%208%5D%2C%20%22voxelCoordinates%22%3A%20%5B34468.0%2C%2041404.0%2C%2041392.0%5D%7D%7D%2C%20%22zoomFactor%22%3A%204%7D%2C%20%22showSlices%22%3A%20false%2C%20%22layout%22%3A%20%22xy-3d%22%7D
Do not extract bare google cloud storage urls, for example gs://fafb-ffn1/main_ng.json are not neuroglancer urls
     """),
    ("human", "User message: {input}")
])

@tool
def extract_ng_payload_from_frag(neuroglancer_url: str) -> dict | None:
    """Extract neuroglancer payload from url fragments
    Arguments:
        neuroglancer_url: a url string looks like: https://spelunker.cave-explorer.org/#!%7B%22layers%22%3A%20%7B%22img%22%3A%20%7B%22source%22%3A%20%22precomputed%3A//gs%3A//
    """
    try:
        components = urllib.parse.urlparse(neuroglancer_url)
        frags = urllib.parse.unquote(components.fragment)
        return json.loads(frags[1:])
    except:
        return None

@tool
def extract_ng_payload_from_gs(gs_url: str) -> dict | None:
    """Extract neuroglancer payload from google storage url
    Arguments:
        gs_url: a google cloud storage url string looks like: gs://fafb-ffn1/main_ng.json, extracted from https://neuroglancer-demo.appspot.com/fafb.html#!gs://fafb-ffn1/main_ng.json
    """
    try:
        data = dl(gs_url)['content']
        return json.loads(data)
    except:
        return None

@tool
def extract_ng_payload_from_ngl(nglstate_url: str) -> dict | None:
    """Extract neuroglancer payload from nglstate url
    Arguments:
        nglstate_url: a nglstate url string looks like https://global.daf-apis.com/nglstate/api/v1/5707813870370816
    """
    # try:
    oauth_token = Variable.get("cave-secret.json", deserialize_json=True)
    headers = {'Authorization': f'Bearer {oauth_token["token"]}'}
    r = requests.get(nglstate_url, headers=headers)
    return json.loads(r.text)
    # except:
    #     return None

def extract_image_layers(payload):
    # try:
    layers = payload["layers"]
    if isinstance(layers, dict):
        layers = [{"name": k, **v} for k, v in layers.items()]

    return [l for l in layers if l["type"] == "image"]
    # except:
    #     return []


class ValidationOutput(TypedDict):
    feedback: str
    validation_flag: Literal["pass", "fail"]


class LLMChatbot:
    def __init__(self):
        self.llm_extraction = None
        self.llm_interface = None
        self.classifier_chain = None
        self.extract_chain = None
        self.guess_chain = None
        self.in_session = False
        self.chat_history = InMemoryChatMessageHistory()
        self.wiki_url = "http://wiki:8080/wiki"
        self.knowledge = self.load_wiki()

    def load_wiki(self):
        all_tiddlers = self.get_all_tiddlers()
        knowledge = {}
        for t in all_tiddlers:
            if 'draft.of' in t:
                continue
            title = t["title"]
            tiddler = self.get_tiddler(title)
            if t.get("type") == "application/json":
                knowledge[title] = json.loads(tiddler.get("text", ""))
            else:
                knowledge[title] = tiddler.get("text", "")

        print(knowledge)

        return knowledge

    def get_all_tiddlers(self):
        """Fetch metadata of all tiddlers in the wiki."""
        url = f"{self.wiki_url}/recipes/default/tiddlers.json"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_tiddler(self, title):
        """Download a specific tiddler (with text)."""
        url = f"{self.wiki_url}/recipes/default/tiddlers/{title}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def reset_session(self):
        """Clears the chat history and session state."""
        self.in_session = False
        self.chat_history.clear()
        print("[Memory]: Session has been reset.")

    def create_chain(self):
        if self.llm_extraction is not None:
            return

        self.tools = [extract_ng_payload_from_frag, extract_ng_payload_from_gs, extract_ng_payload_from_ngl]

        # self.llm_extraction = ChatOpenAI(base_url=base_url, temperature=0.1, api_key=api_key, model=extra_args.get("model", "gpt-4.1-mini"))

        # self.llm_interface = ChatOpenAI(base_url=base_url, temperature=1.0, api_key=api_key, model=extra_args.get("model", "gpt-4.1-mini"))

        self.llm_extraction = ChatVertexAI(model="gemini-2.5-pro", temperature=0.1)

        self.llm_interface = ChatVertexAI(model="gemini-2.5-pro", temperature=1.0)

        self.extract_chain = extraction_prompt | self.llm_extraction.with_structured_output(RawSegParams)

        self.guess_chain = extraction_prompt | self.llm_extraction.with_structured_output(RawSegParams)

        self.exception_chain = exception_prompt | self.llm_interface

        self.classifier_chain = classifier_prompt | self.llm_extraction.with_structured_output(UserIntent)

        self.ng_extraction_chain = nglink_prompt | self.llm_extraction.bind_tools(self.tools)

    def llm_reply(self, event: dict):
        self.create_chain()
        if self.llm_extraction is None:
            return "LLM not available"
        user_input = event['text'].replace(workerid, "").replace(botid, "").lstrip(' ,')
        print(user_input)
        classification_result = self.classifier_chain.invoke(
            {
                "input": user_input,
                "chat_history": self.chat_history.messages,
            },
        )
        print(classification_result)
        if classification_result.intent == IntentEnum.irrelevant:
            self.in_session = False
            return
        else:
            ng_tool_calls = self.ng_extraction_chain.invoke(
                {
                    "input": user_input,
                }
            ).tool_calls
            print(ng_tool_calls)
            if len(ng_tool_calls) > 0:
                tool_call = ng_tool_calls[0]
                args = tool_call['args']
                for tool in self.tools:
                    if tool_call['name'] == tool.name:
                        ng_image_layers = extract_image_layers(tool.run(args))
                        print(ng_image_layers)
            else:
                ng_image_layers = []

        # status = classification_result["conversation_status"]
        # Step 2: Optionally reset memory
        if classification_result.intent == IntentEnum.create_segmentation:
            print("[Memory]: Detected new topic → Clearing history")
            if len(self.chat_history.messages) > 0:
                guessed_result = self.guess_chain.invoke(
                    {
                        "input": user_input,
                        "ng_layers": ng_image_layers,
                        "inferred_params": {},
                        "chat_history": self.chat_history.messages,
                        "knowledge": self.knowledge,
                    },
                )
            else:
                guessed_result = RawSegParams()
            print(f"guessed results: {guessed_result}")
            # self.chat_history = InMemoryChatMessageHistory()
        elif classification_result.intent == IntentEnum.follow_up:
            guessed_result = RawSegParams()
        else:
            return "Not supported yet"

        old_extraction_result = RawSegParams()
        inferred_params = InferredParams()

        for i in range(3):
            print(f"Extraction iteration: {i}")
            print(f"inferred_params: {inferred_params.computed_only()}")
            extraction_result = self.extract_chain.invoke(
                {
                    "input": user_input,
                    "ng_layers": ng_image_layers,
                    "chat_history": self.chat_history.messages,
                    "inferred_params": inferred_params.computed_only(),
                    "knowledge": self.knowledge,
                },
            )
            inferred_params = InferredParams.model_validate(extraction_result.model_dump())
            if extraction_result == old_extraction_result:
                break
            else:
                print(extraction_result)
                print(old_extraction_result)
                old_extraction_result = extraction_result


        self.chat_history.add_user_message(user_input)
        print("extracted parameters:", extraction_result.model_dump())

        try:
            validated = ValidatedSegParams.model_validate(extraction_result.model_dump())
        except ValidationError as e:
            errors = e.errors()
            messages = [
                f"Field `{'.'.join(str(p) for p in err['loc'])}`: {err['msg']}"
                for err in errors
            ]
            print(messages)

            self.chat_history.add_user_message(user_input)
            output = self.exception_chain.invoke(
                {
                    "input": extraction_result.model_dump(),
                    "inferred_params": inferred_params.computed_only(),
                    "errors":  "\n".join(messages),
                    "ng_layers": ng_image_layers,
                    "chat_history": self.chat_history.messages,
                },
            )
            print(output)
            return output.content


        replyto(event, "Starts segmentation")
        self.in_session = False
        feedback = validated.execute(event)
        return feedback
