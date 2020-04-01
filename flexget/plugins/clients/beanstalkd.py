from loguru import logger
from flexget.entry import Entry
from flexget.event import event
from flexget import plugin
import json
logger = logger.bind(name='beanstalkd')

class PluginBeanstalkdBase:
    schema = {
        'anyOf': [
            {'type': 'boolean'},
            {
                'type': 'object',
                'properties': {
                    'host': {'type': 'string'},
                    'port': {'type': 'integer'},
                    'tube': {'type': 'string'},
                    'chunk_size': {'type': 'integer', 'default': 30},
                    'timeout': {'type': 'integer', 'default': 1}
                },
                'required': ['host', 'port', 'tube'],
                'additionalProperties': False,
            },
        ]
    }

    def get_queue(self, beanstalkd_module, config):
        queue = beanstalkd_module.Client(host=config.get('host'), port=config.get('port'), encoding='UTF-8')
        queue.use(config.get('tube'))
        return queue

    @staticmethod
    def get_beanstalkd_module():
        try:
            import greenstalk
        except:
            raise plugin.PluginError('greenstalk module required.', logger)
        return greenstalk


class PluginBeanstalkdInput(PluginBeanstalkdBase):
    def on_task_input(self, task, config):
        beanstalkd_module = self.get_beanstalkd_module()
        queue = self.get_queue(beanstalkd_module, config)
        queue.watch(config.get('tube'))
        beanstalkd_entries = []
        try:
            beanstalkd_entries.append(queue.reserve(config.get('timeout')))
        except beanstalkd_module.TimedOutError:
            pass

        do_continue = True
        entry_count = 0
        entries = []
        while beanstalkd_entries and do_continue:
            for beanstalkd_entry in beanstalkd_entries:
                entry_data = beanstalkd_entry.body
                try:
                    entry = Entry.deserialize(json.loads(entry_data), None)
                    entries.append(entry)
                except json.decoder.JSONDecodeError:
                    logger.error('Could not decode job {beanstalkd_entry.id}')
                    pass
                try:
                    queue.delete(beanstalkd_entry.id)
                    beanstalkd_entries.remove(beanstalkd_entry)
                except beanstalkd_module.NotFoundError:
                    logger.error('Job {beanstalkd_entry.id} has gone away')
                entry_count += 1
            if entry_count > config['chunk_size']:
                do_continue = False
            else:
                try:
                    beanstalkd_entries.append(queue.reserve(config.get('timeout')))
                except beanstalkd_module.TimedOutError:
                    pass
        queue.close()
        return entries


class PluginBeanstalkdOutput(PluginBeanstalkdBase):
    def on_task_output(self, task, config):
        if not task.accepted:
            return

        beanstalkd_module = self.get_beanstalkd_module()
        queue = self.get_queue(beanstalkd_module, config)
        for entry in task.accepted:
            try:
                queue.put(json.dumps(Entry.serialize(entry)).encode('UTF-8'))
            except Exception as e:
                entry.fail(e)
        queue.close()


@event('plugin.register')
def register_plugin():
    plugin.register(PluginBeanstalkdInput, 'from_beanstalkd', api_ver=2)
    plugin.register(PluginBeanstalkdOutput, 'beanstalkd', api_ver=2)